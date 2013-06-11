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

import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import java.io.IOException;

/**
 *
 */
public abstract class ClusterTestBase {

  private InMemoryZKServer zkServer;
  private MiniYARNCluster cluster;
  private WeaveRunnerService runnerService;

  protected final void doInit() throws IOException {
    // Starts Zookeeper
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    // Start YARN mini cluster
    YarnConfiguration config = new YarnConfiguration(new Configuration());

    // TODO: Hack
    config.set("yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler" +
      ".fifo.FifoScheduler");
    config.set("yarn.minicluster.fixed.ports", "true");

    cluster = new MiniYARNCluster("test-cluster", 1, 1, 1);
    cluster.init(config);
    cluster.start();

    runnerService = new YarnWeaveRunnerService(config, zkServer.getConnectionStr() + "/weave",
                                               new LocalLocationFactory(Files.createTempDir()));
    runnerService.startAndWait();
  }

  protected final void doFinish() {
    runnerService.stopAndWait();
    cluster.stop();
    zkServer.stopAndWait();
  }


  protected WeaveRunner getWeaveRunner() {
    return runnerService;
  }
}
