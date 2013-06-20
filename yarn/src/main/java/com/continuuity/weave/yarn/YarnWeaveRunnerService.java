/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.filesystem.HDFSLocationFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.internal.RunIds;
import com.continuuity.weave.internal.SingleRunnableApplication;
import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.continuuity.weave.zookeeper.ZKOperations;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of {@link WeaveRunnerService} that runs application on Yarn cluster.
 */
public final class YarnWeaveRunnerService extends AbstractIdleService implements WeaveRunnerService {

  private static final Logger LOG = LoggerFactory.getLogger(YarnWeaveRunnerService.class);

  private static final int ZK_TIMEOUT = 10000;
  private static final Function<String, RunId> STRING_TO_RUN_ID = new Function<String, RunId>() {
    @Override
    public RunId apply(String input) {
      return RunIds.fromString(input);
    }
  };

  private final YarnClient yarnClient;
  private final ZKClientService zkClientService;
  private final LocationFactory locationFactory;

  // The following two members are for caching calls to lookup and lookpLive.
  private final LoadingCache<String, Iterable<WeaveController>> controllerCache;
  private Iterable<LiveInfo> liveApps;

  public YarnWeaveRunnerService(YarnConfiguration config, String zkConnect) {
    this(config, zkConnect, new HDFSLocationFactory(getFileSystem(config), "/weave"));
  }

  public YarnWeaveRunnerService(YarnConfiguration config, String zkConnect, LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
    this.yarnClient = getYarnClient(config);
    this.zkClientService = getZKClientService(zkConnect);
    this.controllerCache = CacheBuilder.newBuilder().build(createControllerCacheLoader());
  }

  @Override
  public WeavePreparer prepare(WeaveRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public WeavePreparer prepare(WeaveRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public WeavePreparer prepare(WeaveApplication application) {
    Preconditions.checkState(isRunning(), "Service not start. Please call start() first.");
    return new YarnWeavePreparer(application.configure(), yarnClient, zkClientService, locationFactory);
  }

  @Override
  public WeaveController lookup(String applicationName, final RunId runId) {
    return Iterables.tryFind(controllerCache.getUnchecked(applicationName), new Predicate<WeaveController>() {
      @Override
      public boolean apply(WeaveController input) {
        return runId.equals(input.getRunId());
      }
    }).orNull();
  }

  @Override
  public synchronized Iterable<WeaveController> lookup(String applicationName) {
    return controllerCache.getUnchecked(applicationName);
  }

  @Override
  public synchronized Iterable<LiveInfo> lookupLive() {
    if (liveApps != null) {
      return liveApps;
    }

    final Lock lock = new ReentrantLock();
    final Map<String, Cancellable> watched = Maps.newHashMap();
    final Multimap<String, RunId> runIds = HashMultimap.create();
    final CountDownLatch firstFetch = new CountDownLatch(1);

    // Watch child changes in the root, which gives all application names.
    ZKOperations.watchChildren(zkClientService, "/", new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        Set<String> apps = Sets.newHashSet(nodeChildren.getChildren());
        final AtomicInteger count = new AtomicInteger(apps.size());
        // For each for the application name, watch for ephemeral nodes under /instances.
        for (final String appName : apps) {
          if (watched.containsKey(appName)) {
            continue;
          }
          String instancePath = String.format("/%s/instances", appName);
          watched.put(appName, ZKOperations.watchChildren(zkClientService, instancePath,
                                                          new ZKOperations.ChildrenCallback() {
            @Override
            public void updated(NodeChildren nodeChildren) {
              lock.lock();
              try {
                runIds.removeAll(appName);
                // No more child, means no live instances
                if (nodeChildren.getChildren().isEmpty()) {
                  watched.remove(appName).cancel();
                } else {
                  runIds.putAll(appName, Iterables.transform(nodeChildren.getChildren(), STRING_TO_RUN_ID));
                }
              } finally {
                lock.unlock();
              }
              if (firstFetch.getCount() > 0 && count.decrementAndGet() == 0) {
                firstFetch.countDown();
              }
            }
          }));
        }
        // Remove app watches for apps that are gone.
        lock.lock();
        try {
          for (String removeApp : Sets.difference(watched.keySet(), apps)) {
            watched.remove(removeApp).cancel();
            runIds.removeAll(removeApp);
          }
        } finally {
          lock.unlock();
        }
      }
    });

    liveApps = new Iterable <LiveInfo>() {
      @Override
      public Iterator<LiveInfo> iterator() {
        try {
          firstFetch.await();
        } catch (InterruptedException e) {
          LOG.debug("Interrupted exception while waiting for first fetch.", e);
        }
        lock.lock();
        try {
          return getLiveInfos(ImmutableMultimap.copyOf(runIds)).iterator();
        } finally {
          lock.unlock();
        }
      }
    };
    return liveApps;
  }

  @Override
  protected void startUp() throws Exception {
    yarnClient.start();
    zkClientService.startAndWait();
    try {
      // Create the root node, so that the namespace root would get created if it is missing
      zkClientService.create("/", null, CreateMode.PERSISTENT).get();
    } catch (Exception e) {
      // If the exception is caused by node exists, then it's ok. Otherwise propagate the exception.
      Throwable cause = e.getCause();
      if (!(cause instanceof KeeperException) || ((KeeperException) cause).code() != KeeperException.Code.NODEEXISTS) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    zkClientService.stopAndWait();
    yarnClient.stop();
  }

  private ZKClientService getZKClientService(String zkConnect) {
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                   .setSessionTimeout(ZK_TIMEOUT)
                                   .build(), RetryStrategies.exponentialDelay(100, 2000, TimeUnit.MILLISECONDS))));
  }

  private YarnClient getYarnClient(YarnConfiguration config) {
    YarnClient client = new YarnClientImpl();
    client.init(config);
    return client;
  }

  private CacheLoader<String, Iterable<WeaveController>> createControllerCacheLoader() {
    return new CacheLoader<String, Iterable<WeaveController>>() {
      @Override
      public Iterable<WeaveController> load(String applicationName) throws Exception {
        // Namespace the ZKClient to the application namespace
        final ZKClient zkClient = ZKClients.namespace(zkClientService, "/" + applicationName);
        // For storing the live iterable.
        final AtomicReference<Iterable<WeaveController>> controllers =
                      new AtomicReference<Iterable<WeaveController>>(ImmutableList.<WeaveController>of());
        // Latch for blocking until first time fetch completed
        final CountDownLatch firstFetch = new CountDownLatch(1);

        // Watch for chanages under /instances, which container ephemeral nodes created by application master.
        ZKOperations.watchChildren(zkClient, "/instances", new ZKOperations.ChildrenCallback() {
          @Override
          public void updated(NodeChildren nodeChildren) {
            // RunIdToWeaveController(stringToRunId(instanceNode))
            controllers.set(
              Iterables.transform(
                Iterables.transform(nodeChildren.getChildren(), STRING_TO_RUN_ID),
                new Function<RunId, WeaveController>() {
                  @Override
                  public WeaveController apply(RunId runId) {
                    YarnWeaveController controller = new YarnWeaveController(yarnClient, zkClient, null, runId,
                                                                             ImmutableList.<LogHandler>of());
                    controller.start();
                    return controller;
                  }
            }));
            firstFetch.countDown();
          }
        });

        return new Iterable<WeaveController>() {
          @Override
          public Iterator<WeaveController> iterator() {
            try {
              firstFetch.await();
            } catch (InterruptedException e) {
              // OK to ignore.
              LOG.debug("Interrupted exception while waiting for first fetch.", e);
            }
            return controllers.get().iterator();
          }
        };
      }
    };
  }

  private Iterable<LiveInfo> getLiveInfos(Multimap<String, RunId> liveInfos) {
    return Iterables.transform(liveInfos.asMap().entrySet(),
                               new Function<Map.Entry<String, Collection<RunId>>, LiveInfo>() {
      @Override
      public LiveInfo apply(final Map.Entry<String, Collection<RunId>> input) {
        return new LiveInfo() {

          @Override
          public String getApplicationName() {
            return input.getKey();
          }

          @Override
          public Iterable<RunId> getRunIds() {
            return ImmutableList.copyOf(input.getValue());
          }
        };
      }
    });
  }

  private static FileSystem getFileSystem(YarnConfiguration configuration) {
    try {
      return FileSystem.get(configuration);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
