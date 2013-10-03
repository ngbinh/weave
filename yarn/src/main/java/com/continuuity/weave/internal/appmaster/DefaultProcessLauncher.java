/*
 * Copyright 2012-2013 Continuuity,Inc.
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
package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.internal.ContainerInfo;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.ProcessLauncher;
import com.continuuity.weave.internal.yarn.YarnNMClient;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 */
public final class DefaultProcessLauncher implements ProcessLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProcessLauncher.class);

  private final Container container;
  private final YarnNMClient nmClient;
  private final ContainerInfo containerInfo;
  private boolean launched;

  public DefaultProcessLauncher(Container container, YarnNMClient nmClient) {
    this.container = container;
    this.nmClient = nmClient;
    this.containerInfo = new YarnContainerInfo(container);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(ProcessLauncher.class)
      .add("container", container)
      .toString();
  }

  @Override
  public ContainerInfo getContainerInfo() {
    return containerInfo;
  }

  @Override
  public PrepareLaunchContext prepareLaunch(Map<String, String> environment, List<LocalFile> localFiles) {
    return new PrepareLaunchContextImpl(environment, localFiles);
  }

  public boolean isLaunched() {
    return launched;
  }

  public Container getContainer() {
    return container;
  }

  private final class PrepareLaunchContextImpl implements PrepareLaunchContext {

    private final ContainerLaunchContext launchContext;
    private final Map<String, LocalResource> localResources;
    private final Map<String, String> environment;
    private final List<String> commands;

    PrepareLaunchContextImpl(Map<String, String> env, List<LocalFile> localFiles) {
      this.launchContext = Records.newRecord(ContainerLaunchContext.class);
      this.localResources = Maps.newHashMap();
      this.environment = Maps.newHashMap(env);
      this.commands = Lists.newLinkedList();

      for (LocalFile localFile : localFiles) {
        addLocalFile(localFile);
      }
    }

    void addLocalFile(LocalFile localFile) {
      String name = localFile.getName();
      if (localFile.isArchive()) {
        String path = localFile.getURI().toString();
        name += path.endsWith(".tar.gz") ? ".tar.gz" : path.substring(path.lastIndexOf('.'));
      }
      localResources.put(name, YarnUtils.createLocalResource(localFile));
    }

    @Override
    public ResourcesAdder withResources() {
      return new MoreResourcesImpl();
    }

    @Override
    public AfterResources noResources() {
      return new MoreResourcesImpl();
    }

    private final class MoreResourcesImpl implements MoreResources {

      @Override
      public MoreResources add(LocalFile localFile) {
        addLocalFile(localFile);
        return this;
      }

      @Override
      public EnvironmentAdder withEnvironment() {
        return finish();
      }

      @Override
      public AfterEnvironment noEnvironment() {
        return finish();
      }

      private MoreEnvironmentImpl finish() {
        launchContext.setLocalResources(localResources);
        return new MoreEnvironmentImpl();
      }
    }

    private final class MoreEnvironmentImpl implements MoreEnvironment {

      @Override
      public CommandAdder withCommands() {
        // Defaulting extra environments
        environment.put(EnvKeys.YARN_CONTAINER_ID, container.getId().toString());
        environment.put(EnvKeys.YARN_CONTAINER_HOST, container.getNodeId().getHost());
        environment.put(EnvKeys.YARN_CONTAINER_PORT, Integer.toString(container.getNodeId().getPort()));
        environment.put(EnvKeys.YARN_CONTAINER_MEMORY_MB, Integer.toString(container.getResource().getMemory()));
        environment.put(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES,
                        Integer.toString(YarnUtils.getVirtualCores(container.getResource())));

        launchContext.setEnvironment(environment);
        return new MoreCommandImpl();
      }

      @Override
      public <V> MoreEnvironment add(String key, V value) {
        environment.put(key, value.toString());
        return this;
      }
    }

    private final class MoreCommandImpl implements MoreCommand, StdOutSetter, StdErrSetter {

      private final StringBuilder commandBuilder = new StringBuilder();

      @Override
      public StdOutSetter add(String cmd, String... args) {
        commandBuilder.append(cmd);
        for (String arg : args) {
          commandBuilder.append(' ').append(arg);
        }
        return this;
      }

      @Override
      public ProcessController launch() {
        LOG.info("Launching in container {}, {}", container.getId(), commands);
        launchContext.setCommands(commands);

        final Cancellable cancellable = nmClient.start(container, launchContext);
        launched = true;

        return new ProcessController() {
          @Override
          public void kill() {
            cancellable.cancel();
          }
        };
      }

      @Override
      public MoreCommand redirectError(String stderr) {
        commandBuilder.append(' ').append("2>").append(stderr);
        return noError();
      }

      @Override
      public MoreCommand noError() {
        commands.add(commandBuilder.toString());
        commandBuilder.setLength(0);
        return this;
      }

      @Override
      public StdErrSetter redirectOutput(String stdout) {
        commandBuilder.append(' ').append("1>").append(stdout);
        return this;
      }

      @Override
      public StdErrSetter noOutput() {
        return this;
      }
    }
  }
}
