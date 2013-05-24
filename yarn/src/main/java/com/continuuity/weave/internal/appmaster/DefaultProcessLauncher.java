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
package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.ProcessLauncher;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class DefaultProcessLauncher implements ProcessLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProcessLauncher.class);

  private final Container container;
  private final YarnRPC yarnRPC;
  private final YarnConfiguration yarnConf;
  private final String kafkaZKConnect;
  private final List<LocalFile> defaultLocalFiles;
  private final Map<String, String> defaultEnv;

  DefaultProcessLauncher(Container container, YarnRPC yarnRPC, YarnConfiguration yarnConf, String kafkaZKConnect,
                         Iterable<LocalFile> defaultLocalFiles, Map<String, String> defaultEnv) {
    this.container = container;
    this.yarnRPC = yarnRPC;
    this.yarnConf = yarnConf;
    this.kafkaZKConnect = kafkaZKConnect;
    this.defaultEnv = ImmutableMap.copyOf(defaultEnv);
    this.defaultLocalFiles = ImmutableList.copyOf(defaultLocalFiles);
  }

  @Override
  public PrepareLaunchContext prepareLaunch() {
    return new PrepareLaunchContextImpl();
  }

  /**
   * Helper to connect to container manager (node manager).
   */
  private ContainerManager connectContainerManager() {
    String cmIpPortStr = String.format("%s:%d", container.getNodeId().getHost(), container.getNodeId().getPort());
    InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
    return ((ContainerManager) yarnRPC.getProxy(ContainerManager.class, cmAddress, yarnConf));
  }


  private final class PrepareLaunchContextImpl implements PrepareLaunchContext {

    private final ContainerLaunchContext launchContext;
    private final Map<String, LocalResource> localResources;
    private final Map<String, String> environment;
    private final List<String> commands;

    PrepareLaunchContextImpl() {
      this.launchContext = Records.newRecord(ContainerLaunchContext.class);
      launchContext.setContainerId(container.getId());
      launchContext.setResource(container.getResource());

      this.localResources = Maps.newHashMap();
      this.environment = Maps.newHashMap();
      this.commands = Lists.newLinkedList();
    }

    @Override
    public AfterUser setUser(String user) {
      launchContext.setUser(user);
      return new AfterUserImpl();
    }

    private final class AfterUserImpl implements AfterUser {

      @Override
      public ResourcesAdder withResources() {
        return new MoreResourcesImpl();
      }

      @Override
      public AfterResources noResources() {
        return new MoreResourcesImpl();
      }
    }

    private final class MoreResourcesImpl implements MoreResources {

      private MoreResourcesImpl() {
        for (LocalFile localFile : defaultLocalFiles) {
          add(localFile);
        }
      }

      @Override
      public MoreResources add(LocalFile localFile) {
        String name = localFile.getName();
        if (localFile.isArchive()) {
          String path = localFile.getURI().toString();
          name += path.endsWith(".tar.gz") ? ".tar.gz" : path.substring(path.lastIndexOf('.'));
        }
        localResources.put(name, YarnUtils.createLocalResource(localFile));
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

      private MoreEnvironmentImpl() {
        environment.putAll(defaultEnv);
      }

      @Override
      public CommandAdder withCommands() {
        // Defaulting extra environments
        environment.put(EnvKeys.YARN_CONTAINER_ID, container.getId().toString());
        environment.put(EnvKeys.YARN_CONTAINER_HOST, container.getNodeId().getHost());
        environment.put(EnvKeys.YARN_CONTAINER_PORT, Integer.toString(container.getNodeId().getPort()));
        environment.put(EnvKeys.WEAVE_LOG_KAFKA_ZK, kafkaZKConnect);

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
        LOG.info("Launching in container " + container.getId() + ": " + commands);
        launchContext.setCommands(commands);

        StartContainerRequest startRequest = Records.newRecord(StartContainerRequest.class);
        startRequest.setContainerLaunchContext(launchContext);
        ContainerManager containerManager = connectContainerManager();
        try {
          containerManager.startContainer(startRequest);
          return new ProcessControllerImpl(containerManager);
        } catch (YarnRemoteException e) {
          LOG.error("Error in launching process", e);
          throw Throwables.propagate(e);
        }
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

  private final class ProcessControllerImpl implements ProcessController {

    private final ContainerManager containerManager;

    private ProcessControllerImpl(ContainerManager containerManager) {
      this.containerManager = containerManager;
    }

    @Override
    public void kill() {
      LOG.info("Request to kill container: " + container.getId());
      StopContainerRequest stopRequest = Records.newRecord(StopContainerRequest.class);
      stopRequest.setContainerId(container.getId());
      try {
        containerManager.stopContainer(stopRequest);
        boolean completed = false;
        while (!completed) {
          GetContainerStatusRequest statusRequest = Records.newRecord(GetContainerStatusRequest.class);
          statusRequest.setContainerId(container.getId());
          GetContainerStatusResponse statusResponse = containerManager.getContainerStatus(statusRequest);
          LOG.info("Container status: {} {}", statusResponse.getStatus(), statusResponse.getStatus().getDiagnostics());

          completed = (statusResponse.getStatus().getState() == ContainerState.COMPLETE);
        }
      } catch (YarnRemoteException e) {
        LOG.error("Fail to kill container.", e);
        throw Throwables.propagate(e);
      }
    }
  }
}
