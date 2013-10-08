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
package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.ProcessController;
import com.continuuity.weave.internal.ProcessLauncher;
import com.continuuity.weave.internal.appmaster.ApplicationMasterProcessLauncher;
import com.continuuity.weave.internal.appmaster.ApplicationSubmitter;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class Hadoop20YarnAppClient extends AbstractIdleService implements YarnAppClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop20YarnAppClient.class);
  private final YarnClient yarnClient;

  public Hadoop20YarnAppClient(Configuration configuration) {
    this.yarnClient = new YarnClientImpl();
    yarnClient.init(configuration);
  }

  @Override
  public ProcessLauncher<ApplicationId> createLauncher(WeaveSpecification weaveSpec) throws Exception {
    // Request for new application
    GetNewApplicationResponse response = yarnClient.getNewApplication();
    final ApplicationId appId = response.getApplicationId();

    // Setup the context for application submission
    final ApplicationSubmissionContext appSubmissionContext = Records.newRecord(ApplicationSubmissionContext.class);
    appSubmissionContext.setApplicationId(appId);
    appSubmissionContext.setApplicationName(weaveSpec.getName());
    appSubmissionContext.setUser(System.getProperty("user.name"));

    ApplicationSubmitter submitter = new ApplicationSubmitter() {

      @Override
      public ProcessController<ApplicationReport> submit(ContainerLaunchContext launchContext, Resource capability) {
        launchContext.setUser(appSubmissionContext.getUser());
        launchContext.setResource(capability);
        appSubmissionContext.setAMContainerSpec(launchContext);

        try {
          yarnClient.submitApplication(appSubmissionContext);
          return new ProcessControllerImpl(yarnClient, appId);
        } catch (YarnRemoteException e) {
          LOG.error("Failed to submit application {}", appId, e);
          throw Throwables.propagate(e);
        }
      }
    };

    return new ApplicationMasterProcessLauncher(appId, submitter);
  }

  @Override
  public ProcessController<ApplicationReport> createProcessController(ApplicationId appId) {
    return new ProcessControllerImpl(yarnClient, appId);
  }

  @Override
  protected void startUp() throws Exception {
    yarnClient.start();
  }

  @Override
  protected void shutDown() throws Exception {
    yarnClient.stop();
  }

  private static final class ProcessControllerImpl implements ProcessController<ApplicationReport> {
    private final YarnClient yarnClient;
    private final ApplicationId appId;

    public ProcessControllerImpl(YarnClient yarnClient, ApplicationId appId) {
      this.yarnClient = yarnClient;
      this.appId = appId;
    }

    @Override
    public ApplicationReport getReport() {
      try {
        return yarnClient.getApplicationReport(appId);
      } catch (YarnRemoteException e) {
        LOG.error("Failed to get application report {}", appId, e);
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void cancel() {
      try {
        yarnClient.killApplication(appId);
      } catch (YarnRemoteException e) {
        LOG.error("Failed to kill application {}", appId, e);
        throw Throwables.propagate(e);
      }
    }
  }
}
