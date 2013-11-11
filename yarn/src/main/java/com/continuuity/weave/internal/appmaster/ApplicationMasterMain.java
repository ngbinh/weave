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

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.Constants;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.RunIds;
import com.continuuity.weave.internal.ServiceMain;
import com.continuuity.weave.internal.yarn.VersionDetectYarnAMClientFactory;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * Main class for launching {@link ApplicationMasterService}.
 */
public final class ApplicationMasterMain extends ServiceMain {

  private final String kafkaZKConnect;

  private ApplicationMasterMain(String kafkaZKConnect) {
    this.kafkaZKConnect = kafkaZKConnect;
  }

  /**
   * Starts the application master.
   */
  public static void main(String[] args) throws Exception {
    String zkConnect = System.getenv(EnvKeys.WEAVE_ZK_CONNECT);
    File weaveSpec = new File(Constants.Files.WEAVE_SPEC);
    RunId runId = RunIds.fromString(System.getenv(EnvKeys.WEAVE_RUN_ID));

    ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zkConnect).build(),
            RetryStrategies.fixDelay(1, TimeUnit.SECONDS))));

    Configuration conf = new YarnConfiguration(new HdfsConfiguration(new Configuration()));
    Service service = new ApplicationMasterService(runId, zkClientService, weaveSpec,
                                                   new VersionDetectYarnAMClientFactory(conf), createAppLocation(conf));
    new ApplicationMasterMain(String.format("%s/%s/kafka", zkConnect, runId.getId())).doMain(zkClientService, service);
  }

  @Override
  protected String getHostname() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      return "unknown";
    }
  }

  @Override
  protected String getKafkaZKConnect() {
    return kafkaZKConnect;
  }
}
