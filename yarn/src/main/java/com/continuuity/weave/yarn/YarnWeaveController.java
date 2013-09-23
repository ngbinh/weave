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
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.ResourceReport;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.internal.AbstractWeaveController;
import com.continuuity.weave.internal.Constants;
import com.continuuity.weave.internal.appmaster.ResourceReportClient;
import com.continuuity.weave.internal.state.StateNode;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * A {@link WeaveController} that controllers application running on Hadoop YARN.
 */
final class YarnWeaveController extends AbstractWeaveController implements WeaveController {

  private static final Logger LOG = LoggerFactory.getLogger(YarnWeaveController.class);

  private final YarnClient yarnClient;
  private final ApplicationId applicationId;
  private final Runnable startUp;
  private ResourceReportClient resourcesClient;

  YarnWeaveController(RunId runId, ZKClient zkClient, YarnClient yarnClient, ApplicationId appId) {
    this(runId, zkClient, ImmutableList.<LogHandler>of(), yarnClient, appId, new Runnable() {
      @Override
      public void run() {
        // No-op
      }
    });
  }


  YarnWeaveController(RunId runId, ZKClient zkClient, Iterable<LogHandler> logHandlers,
                      YarnClient yarnClient, ApplicationId appId, Runnable startUp) {
    super(runId, zkClient, logHandlers);
    this.yarnClient = yarnClient;
    this.applicationId = appId;
    this.startUp = startUp;
  }

  @Override
  protected void doStartUp() {
    super.doStartUp();
    startUp.run();

    // Poll the status of the yarn application
    try {
      YarnApplicationState state = yarnClient.getApplicationReport(applicationId).getYarnApplicationState();
      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      stopWatch.split();
      long maxTime = TimeUnit.MILLISECONDS.convert(Constants.APPLICATION_MAX_START_SECONDS, TimeUnit.SECONDS);

      LOG.info("Checking yarn application status");
      while (!hasRun(state) && stopWatch.getSplitTime() < maxTime) {
        state = yarnClient.getApplicationReport(applicationId).getYarnApplicationState();
        LOG.debug("Yarn application status: {}", state);
        TimeUnit.SECONDS.sleep(1);
        stopWatch.split();
      }
      LOG.info("Yarn application is in state {}", state);
      if (state != YarnApplicationState.RUNNING) {
        LOG.info("Yarn application is not in running state. Shutting down controller.",
                 Constants.APPLICATION_MAX_START_SECONDS);
        forceShutDown();
      } else {
        String appMasterHost = yarnClient.getApplicationReport(applicationId).getHost();
        int appMasterPort = yarnClient.getApplicationReport(applicationId).getRpcPort();
        resourcesClient = new ResourceReportClient(appMasterHost, appMasterPort);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void doShutDown() {
    // Wait for the stop message being processed
    try {
      Uninterruptibles.getUninterruptibly(getStopMessageFuture(),
                                          Constants.APPLICATION_MAX_STOP_SECONDS, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Failed to wait for stop message being processed.", e);
      // Kill the application through yarn
      kill();
    }

    // Poll application status from yarn
    try {
      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      stopWatch.split();
      long maxTime = TimeUnit.MILLISECONDS.convert(Constants.APPLICATION_MAX_STOP_SECONDS, TimeUnit.SECONDS);

      LOG.info("Fetching yarn application report for {}", applicationId);
      FinalApplicationStatus finalStatus = yarnClient.getApplicationReport(applicationId).getFinalApplicationStatus();
      while (finalStatus == FinalApplicationStatus.UNDEFINED && stopWatch.getSplitTime() < maxTime) {
        LOG.debug("Yarn application final status for {} {}", applicationId, finalStatus);
        TimeUnit.SECONDS.sleep(1);
        stopWatch.split();
        finalStatus = yarnClient.getApplicationReport(applicationId).getFinalApplicationStatus();
      }
      LOG.debug("Yarn application final status is {}", finalStatus);

      // Application not finished after max stop time, kill the application
      if (finalStatus == FinalApplicationStatus.UNDEFINED) {
        kill();
      }
    } catch (Exception e) {
      LOG.warn("Exception while waiting for application report: {}", e.getMessage(), e);
      kill();
    }

    super.doShutDown();
  }

  @Override
  public void kill() {
    try {
      LOG.info("Killing application {}", applicationId);
      yarnClient.killApplication(applicationId);
    } catch (YarnRemoteException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void instanceNodeUpdated(NodeData nodeData) {

  }

  @Override
  protected void stateNodeUpdated(StateNode stateNode) {

  }

  private boolean hasRun(YarnApplicationState state) {
    switch (state) {
      case RUNNING:
      case FINISHED:
      case FAILED:
      case KILLED:
        return true;
    }
    return false;
  }

  @Override
  public ResourceReport getResourceReport() {
    // in case the user calls this before starting, return null
    return (resourcesClient == null) ? null : resourcesClient.get();
  }
}
