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
package com.continuuity.weave.internal;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.StateNode;
import com.continuuity.weave.launcher.WeaveLauncher;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * This class helps launching a container.
 */
public final class WeaveContainerLauncher {

  private static final double HEAP_MIN_RATIO = 0.7d;

  private final RuntimeSpecification runtimeSpec;
  private final ProcessLauncher.PrepareLaunchContext launchContext;
  private final ZKClient zkClient;
  private final int instanceCount;
  private final String jvmOpts;
  private final int reservedMemory;

  public WeaveContainerLauncher(RuntimeSpecification runtimeSpec, ProcessLauncher.PrepareLaunchContext launchContext,
                                ZKClient zkClient, int instanceCount, String jvmOpts, int reservedMemory) {
    this.runtimeSpec = runtimeSpec;
    this.launchContext = launchContext;
    this.zkClient = zkClient;
    this.instanceCount = instanceCount;
    this.jvmOpts = jvmOpts;
    this.reservedMemory = reservedMemory;
  }

  public WeaveContainerController start(RunId runId, int instanceId) {
    ProcessLauncher.PrepareLaunchContext.AfterResources afterResources = null;
    if (runtimeSpec.getLocalFiles().isEmpty()) {
      afterResources = launchContext.noResources();
    } else {
      ProcessLauncher.PrepareLaunchContext.ResourcesAdder resourcesAdder = launchContext.withResources();

      for (LocalFile localFile : runtimeSpec.getLocalFiles()) {
        afterResources = resourcesAdder.add(localFile);
      }
    }

    int memory = runtimeSpec.getResourceSpecification().getMemorySize();
    if (((double) (memory - reservedMemory) / memory) >= HEAP_MIN_RATIO) {
      // Reduce -Xmx by the reserved memory size.
      memory = runtimeSpec.getResourceSpecification().getMemorySize() - reservedMemory;
    } else {
      // If it is a small VM, just discount it by the min ratio.
      memory = (int) Math.ceil(memory * HEAP_MIN_RATIO);
    }

    // Currently no reporting is supported for runnable containers
    ProcessController<Void> processController = afterResources
      .withEnvironment()
      .add(EnvKeys.WEAVE_RUN_ID, runId.getId())
      .add(EnvKeys.WEAVE_RUNNABLE_NAME, runtimeSpec.getName())
      .add(EnvKeys.WEAVE_INSTANCE_ID, Integer.toString(instanceId))
      .add(EnvKeys.WEAVE_INSTANCE_COUNT, Integer.toString(instanceCount))
      .withCommands()
      .add("java",
           "-Djava.io.tmpdir=tmp",
           "-Dyarn.container=$" + EnvKeys.YARN_CONTAINER_ID,
           "-Dweave.runnable=$" + EnvKeys.WEAVE_APP_NAME + ".$" + EnvKeys.WEAVE_RUNNABLE_NAME,
           "-cp", Constants.Files.LAUNCHER_JAR,
           "-Xmx" + memory + "m",
           jvmOpts,
           WeaveLauncher.class.getName(),
           Constants.Files.CONTAINER_JAR,
           WeaveContainerMain.class.getName(),
           Boolean.TRUE.toString())
      .redirectOutput(Constants.STDOUT).redirectError(Constants.STDERR)
      .launch();

    WeaveContainerControllerImpl controller = new WeaveContainerControllerImpl(zkClient, runId, processController);
    controller.start();
    return controller;
  }

  private static final class WeaveContainerControllerImpl extends AbstractZKServiceController
                                                          implements WeaveContainerController {

    private final ProcessController<Void> processController;

    protected WeaveContainerControllerImpl(ZKClient zkClient, RunId runId,
                                           ProcessController<Void> processController) {
      super(runId, zkClient);
      this.processController = processController;
    }

    @Override
    protected void doStartUp() {
      // No-op
    }

    @Override
    protected void doShutDown() {
      // No-op
    }

    @Override
    protected void instanceNodeUpdated(NodeData nodeData) {
      // No-op
    }

    @Override
    protected void stateNodeUpdated(StateNode stateNode) {
      // No-op
    }

    @Override
    public ListenableFuture<Message> sendMessage(Message message) {
      return sendMessage(message, message);
    }

    @Override
    public synchronized void completed(int exitStatus) {
      if (exitStatus != 0) {  // If a container terminated with exit code != 0, treat it as error
//        fireStateChange(new StateNode(State.FAILED, new StackTraceElement[0]));
      }
      forceShutDown();
    }

    @Override
    public void kill() {
      processController.cancel();
    }
  }
}
