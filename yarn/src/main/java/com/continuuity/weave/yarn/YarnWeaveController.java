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
import com.continuuity.weave.internal.ProcessController;
import com.continuuity.weave.internal.appmaster.TrackerService;
import com.continuuity.weave.internal.state.StateNode;
import com.continuuity.weave.internal.state.SystemMessages;
import com.continuuity.weave.internal.yarn.YarnApplicationReport;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A {@link WeaveController} that controllers application running on Hadoop YARN.
 */
final class YarnWeaveController extends AbstractWeaveController implements WeaveController {

  private static final Logger LOG = LoggerFactory.getLogger(YarnWeaveController.class);

  private final Callable<ProcessController<YarnApplicationReport>> startUp;
  private ProcessController<YarnApplicationReport> processController;
  private ResourceReportClient resourcesClient;

  /**
   * Creates an instance without any {@link LogHandler}.
   */
  YarnWeaveController(RunId runId, ZKClient zkClient, Callable<ProcessController<YarnApplicationReport>> startUp) {
    this(runId, zkClient, ImmutableList.<LogHandler>of(), startUp);
  }

  YarnWeaveController(RunId runId, ZKClient zkClient, Iterable <LogHandler> logHandlers,
                      Callable<ProcessController<YarnApplicationReport>> startUp) {
    super(runId, zkClient, logHandlers);
    this.startUp = startUp;
  }


  /**
   * Sends a message to application to notify the secure store has be updated.
   */
  ListenableFuture<Void> secureStoreUpdated() {
    return sendMessage(SystemMessages.SECURE_STORE_UPDATED, null);
  }

  @Override
  protected void doStartUp() {
    super.doStartUp();

    // Submit and poll the status of the yarn application
    try {
      processController = startUp.call();

      YarnApplicationReport report = processController.getReport();
      LOG.debug("Application {} submit", report.getApplicationId());

      YarnApplicationState state = report.getYarnApplicationState();
      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      stopWatch.split();
      long maxTime = TimeUnit.MILLISECONDS.convert(Constants.APPLICATION_MAX_START_SECONDS, TimeUnit.SECONDS);

      LOG.info("Checking yarn application status");
      while (!hasRun(state) && stopWatch.getSplitTime() < maxTime) {
        report = processController.getReport();
        state = report.getYarnApplicationState();
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
        try {
          URL resourceUrl = URI.create(String.format("http://%s:%d", report.getHost(), report.getRpcPort()))
                               .resolve(TrackerService.PATH).toURL();
          resourcesClient = new ResourceReportClient(resourceUrl);
        } catch (IOException e) {
          resourcesClient = null;
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void doShutDown() {
    if (processController == null) {
      LOG.warn("No process controller for application that is not submitted.");
      return;
    }

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

      YarnApplicationReport report = processController.getReport();
      FinalApplicationStatus finalStatus = report.getFinalApplicationStatus();
      while (finalStatus == FinalApplicationStatus.UNDEFINED && stopWatch.getSplitTime() < maxTime) {
        LOG.debug("Yarn application final status for {} {}", report.getApplicationId(), finalStatus);
        TimeUnit.SECONDS.sleep(1);
        stopWatch.split();
        finalStatus = processController.getReport().getFinalApplicationStatus();
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
    if (processController != null) {
      YarnApplicationReport report = processController.getReport();
      LOG.info("Killing application {}", report.getApplicationId());
      processController.cancel();
    } else {
      LOG.warn("No process controller for application that is not submitted.");
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
