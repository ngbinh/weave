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

import com.continuuity.weave.internal.Constants;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.ProcessController;
import com.continuuity.weave.internal.yarn.AbstractYarnProcessLauncher;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * A {@link com.continuuity.weave.internal.ProcessLauncher} for launching Application Master from the client.
 */
public final class ApplicationMasterProcessLauncher extends AbstractYarnProcessLauncher<ApplicationId> {

  private final ApplicationSubmitter submitter;

  public ApplicationMasterProcessLauncher(ApplicationId appId, ApplicationSubmitter submitter) {
    super(appId);
    this.submitter = submitter;
  }

  @Override
  protected boolean useArchiveSuffix() {
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <R> ProcessController<R> doLaunch(ContainerLaunchContext launchContext) {
    final ApplicationId appId = getContainerInfo();

    // Set the resource requirement for AM
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(Constants.APP_MASTER_MEMORY_MB);
    YarnUtils.setVirtualCores(capability, 1);

    // Put in extra environments
    Map<String, String> env = Maps.newHashMap(launchContext.getEnvironment());
    env.put(EnvKeys.WEAVE_APP_ID, Integer.toString(appId.getId()));
    env.put(EnvKeys.WEAVE_APP_ID_CLUSTER_TIME, Long.toString(appId.getClusterTimestamp()));
    env.put(EnvKeys.YARN_CONTAINER_MEMORY_MB, Integer.toString(Constants.APP_MASTER_MEMORY_MB));
    env.put(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES, Integer.toString(YarnUtils.getVirtualCores(capability)));

    launchContext.setEnvironment(env);
    return (ProcessController<R>) submitter.submit(launchContext, capability);
  }
}
