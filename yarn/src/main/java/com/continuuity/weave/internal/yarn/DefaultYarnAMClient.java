/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.internal.appmaster.TrackerService;
import com.continuuity.weave.internal.yarn.ports.AMRMClient;
import com.continuuity.weave.internal.yarn.ports.AMRMClientImpl;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.net.InetSocketAddress;

/**
 *
 */
public final class DefaultYarnAMClient extends AbstractExecutionThreadService {

  private final YarnConfiguration yarnConf;
  private final YarnRPC yarnRPC;
  private final TrackerService trackerService;
  private final AMRMClient amrmClient;

  public DefaultYarnAMClient(YarnConfiguration yarnConf,
                             ApplicationAttemptId attemptId,
                             TrackerService trackerService) {
    this.yarnConf = yarnConf;
    this.yarnRPC = YarnRPC.create(yarnConf);
    this.trackerService = trackerService;
    this.amrmClient = new AMRMClientImpl(attemptId);
    this.amrmClient.init(yarnConf);
  }

  @Override
  protected void startUp() throws Exception {
    amrmClient.start();

    InetSocketAddress trackerAddr = trackerService.getBindAddress();
    amrmClient.registerApplicationMaster(trackerAddr.getHostName(), trackerAddr.getPort(),
                                         trackerService.getUrl().toString());
  }

  @Override
  protected void shutDown() throws Exception {
    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, trackerService.getUrl().toString());
    amrmClient.stop();
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {

    }
  }
}
