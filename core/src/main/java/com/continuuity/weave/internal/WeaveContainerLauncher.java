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
package com.continuuity.weave.internal;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.launcher.WeaveLauncher;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.collect.ImmutableList;

/**
 *
 */
public final class WeaveContainerLauncher {

  private final RuntimeSpecification runtimeSpec;
  private final RunId runId;
  private final ProcessLauncher processLauncher;
  private final ZKClient zkClient;
  private final Iterable<String> args;
  private final int instanceId;

  public WeaveContainerLauncher(RuntimeSpecification runtimeSpec, RunId runId, ProcessLauncher processLauncher,
                                ZKClient zkClient, Iterable<String> args, int instanceId) {
    this.runtimeSpec = runtimeSpec;
    this.runId = runId;
    this.processLauncher = processLauncher;
    this.zkClient = zkClient;
    this.args = args;
    this.instanceId = instanceId;
  }

  public ServiceController start(String stdout, String stderr) {
    ProcessLauncher.PrepareLaunchContext.AfterUser afterUser = processLauncher.prepareLaunch()
      .setUser(System.getProperty("user.name"));

    ProcessLauncher.PrepareLaunchContext.AfterResources afterResources = null;
    if (runtimeSpec.getLocalFiles().isEmpty()) {
      afterResources = afterUser.noResources();
    }

    ProcessLauncher.PrepareLaunchContext.ResourcesAdder resourcesAdder = afterUser.withResources();

    for (LocalFile localFile : runtimeSpec.getLocalFiles()) {
      afterResources = resourcesAdder.add(localFile);
    }

    int memory = runtimeSpec.getResourceSpecification().getMemorySize();

    final ProcessLauncher.ProcessController processController = afterResources
      .withEnvironment()
        .add(EnvKeys.WEAVE_RUN_ID, runId.getId())
        .add(EnvKeys.WEAVE_RUNNABLE_NAME, runtimeSpec.getName())
        .add(EnvKeys.WEAVE_INSTANCE_ID, Integer.toString(instanceId))
      .withCommands()
        .add("java",
             ImmutableList.<String>builder()
               .add("-cp").add("launcher.jar")
               .add("-Xmx" + memory + "m")
               .add(WeaveLauncher.class.getName())
               .add("container.jar")
               .add(WeaveContainerMain.class.getName())
               .add(Boolean.TRUE.toString())
               .addAll(args).build().toArray(new String[0]))
      .redirectOutput(stdout).redirectError(stderr)
      .launch();

    AbstractServiceController controller = new AbstractServiceController(zkClient, runId) {

      @Override
      public void kill() {
        processController.kill();
      }
    };
    controller.start();
    return controller;
  }
}
