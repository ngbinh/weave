/**
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

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.internal.DefaultLocalFile;
import com.continuuity.weave.internal.DefaultWeaveRunnableSpecification;
import com.continuuity.weave.internal.DefaultWeaveSpecification;
import com.continuuity.weave.internal.SingleRunnableApplication;
import com.continuuity.weave.internal.ZKWeaveController;
import com.continuuity.weave.filesystem.HDFSLocationFactory;
import com.continuuity.weave.internal.logging.KafkaWeaveRunnable;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class YarnWeaveRunnerService extends AbstractIdleService implements WeaveRunnerService {

  private static final String KAFKA_ARCHIVE = "kafka-0.7.2.tgz";
  private static final int ZK_TIMEOUT = 10000;

  private final YarnClient yarnClient;
  private final ZKClientService zkClientService;
  private final LocationFactory locationFactory;

  public YarnWeaveRunnerService(YarnConfiguration config, String zkConnect) {
    try {
      this.locationFactory = new HDFSLocationFactory(FileSystem.get(config), "/weave");
      this.yarnClient = getYarnClient(config);
      this.zkClientService = getZKClientService(zkConnect);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public YarnWeaveRunnerService(YarnConfiguration config, String zkConnect, LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
    this.yarnClient = getYarnClient(config);
    this.zkClientService = getZKClientService(zkConnect);
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
    return new YarnWeavePreparer(addKafka(application.configure()), yarnClient, zkClientService, locationFactory);
  }

  @Override
  public WeaveController lookup(String applicationName, RunId runId) {
    ZKClient zkClient = ZKClients.namespace(zkClientService, "/" + applicationName);
    ZKWeaveController controller = new ZKWeaveController(zkClient, runId, ImmutableList.<LogHandler>of());
    controller.start();
    return controller;
  }

  @Override
  protected void startUp() throws Exception {
    yarnClient.start();
    zkClientService.startAndWait();
    try {
      // Create the root node, so that the namespace root would get created it is missing
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

  // Add the kafka runnable.
  // TODO: It is a bit hacky to just add it in here
  private WeaveSpecification addKafka(final WeaveSpecification weaveSpec) {
    final String kafkaName = "kafka";

    return new WeaveSpecification() {
      @Override
      public String getName() {
        return weaveSpec.getName();
      }

      @Override
      public Map<String, RuntimeSpecification> getRunnables() {
        RuntimeSpecification kafkaRuntimeSpec = new RuntimeSpecification() {

          @Override
          public String getName() {
            return kafkaName;
          }

          @Override
          public WeaveRunnableSpecification getRunnableSpecification() {
            KafkaWeaveRunnable kafkaRunnable = new KafkaWeaveRunnable("kafka.tgz");
            return new DefaultWeaveRunnableSpecification(kafkaRunnable.getClass().getName(),
                                                         kafkaRunnable.configure());
          }

          @Override
          public ResourceSpecification getResourceSpecification() {
            return ResourceSpecification.Builder.with()
              .setCores(1).setMemory(1, ResourceSpecification.SizeUnit.GIGA).build();
          }

          @Override
          public Collection<LocalFile> getLocalFiles() {
            try {
              URL kafkaArchive = getClass().getClassLoader().getResource(KAFKA_ARCHIVE);
              LocalFile kafka = new DefaultLocalFile("kafka.tgz", kafkaArchive.toURI(), -1, -1, true, null);
              return ImmutableList.of(kafka);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        };

        return ImmutableMap.<String, RuntimeSpecification>builder()
                      .putAll(weaveSpec.getRunnables())
                      .put(kafkaName, kafkaRuntimeSpec)
                      .build();
      }

      @Override
      public List<Order> getOrders() {
        ImmutableList.Builder<Order> orders = ImmutableList.builder();
        orders.add(new DefaultWeaveSpecification.DefaultOrder(ImmutableSet.of(kafkaName), Order.Type.STARTED));
        orders.addAll(weaveSpec.getOrders());
        return orders.build();
      }
    };
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
}
