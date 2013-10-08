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

import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.internal.ContainerInfo;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.ProcessController;
import com.continuuity.weave.internal.yarn.AbstractYarnProcessLauncher;
import com.continuuity.weave.internal.yarn.YarnNMClient;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 */
public final class RunnableProcessLauncher extends AbstractYarnProcessLauncher<ContainerInfo> {

  private static final Logger LOG = LoggerFactory.getLogger(RunnableProcessLauncher.class);

  private final Container container;
  private final YarnNMClient nmClient;
  private boolean launched;

  public RunnableProcessLauncher(Container container, YarnNMClient nmClient) {
    super(new YarnContainerInfo(container));
    this.container = container;
    this.nmClient = nmClient;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("container", container)
      .toString();
  }

  @Override
  protected <R> ProcessController<R> doLaunch(ContainerLaunchContext launchContext) {
    Map<String, String> env = Maps.newHashMap(launchContext.getEnvironment());

    // Set extra environments
    env.put(EnvKeys.YARN_CONTAINER_ID, container.getId().toString());
    env.put(EnvKeys.YARN_CONTAINER_HOST, container.getNodeId().getHost());
    env.put(EnvKeys.YARN_CONTAINER_PORT, Integer.toString(container.getNodeId().getPort()));
    env.put(EnvKeys.YARN_CONTAINER_MEMORY_MB, Integer.toString(container.getResource().getMemory()));
    env.put(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES,
                    Integer.toString(YarnUtils.getVirtualCores(container.getResource())));

    launchContext.setEnvironment(env);

    LOG.info("Launching in container {}, {}", container.getId(), launchContext.getCommands());
    final Cancellable cancellable = nmClient.start(container, launchContext);
    launched = true;

    return new ProcessController<R>() {
      @Override
      public R getReport() {
        // No reporting support for runnable launch yet.
        return null;

      }

      @Override
      public void cancel() {
        cancellable.cancel();
      }
    };
  }

  public boolean isLaunched() {
    return launched;
  }

  public Container getContainer() {
    return container;
  }
}
