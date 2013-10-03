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

import com.continuuity.weave.internal.ProcessLauncher;
import com.continuuity.weave.internal.appmaster.DefaultProcessLauncher;
import com.continuuity.weave.internal.appmaster.TrackerService;
import com.continuuity.weave.internal.yarn.ports.AMRMClient;
import com.continuuity.weave.internal.yarn.ports.AMRMClientImpl;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;

/**
 *
 */
public final class Hadoop20YarnAMClient extends AbstractIdleService implements YarnAMClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop20YarnAMClient.class);

  private final TrackerService trackerService;
  private final AMRMClient amrmClient;
  private final YarnNMClient nmClient;
  private Resource maxCapability;
  private Resource minCapability;

  public Hadoop20YarnAMClient(Configuration conf, ApplicationAttemptId attemptId, TrackerService trackerService) {
    this.trackerService = trackerService;
    this.amrmClient = new AMRMClientImpl(attemptId);
    this.amrmClient.init(conf);
    this.nmClient = new Hadoop20YarnNMClient(YarnRPC.create(conf), conf);
  }

  @Override
  protected void startUp() throws Exception {
    amrmClient.start();

    InetSocketAddress trackerAddr = trackerService.getBindAddress();
    URL trackerUrl = trackerService.getUrl();
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster(trackerAddr.getHostName(),
                                                                                      trackerAddr.getPort(),
                                                                                      trackerUrl.toString());
    maxCapability = response.getMaximumResourceCapability();
    minCapability = response.getMinimumResourceCapability();
  }

  @Override
  protected void shutDown() throws Exception {
    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, trackerService.getUrl().toString());
    amrmClient.stop();
  }

  @Override
  public synchronized void allocate(float progress, AllocateHandler handler) throws Exception {
    AMResponse amResponse = amrmClient.allocate(progress).getAMResponse();
    List<ProcessLauncher> launchers = Lists.newArrayListWithCapacity(amResponse.getAllocatedContainers().size());
    for (Container container : amResponse.getAllocatedContainers()) {
      launchers.add(new DefaultProcessLauncher(container, nmClient));
    }

    if (!launchers.isEmpty()) {
      handler.acquired(launchers);

      // If no process has been launched through the given launcher, return the container.
      for (ProcessLauncher l : launchers) {
        DefaultProcessLauncher launcher = (DefaultProcessLauncher) l;
        if (!launcher.isLaunched()) {
          Container container = launcher.getContainer();
          LOG.info("Nothing to run in container, releasing it: {}", container);
          amrmClient.releaseAssignedContainer(container.getId());
        }
      }
    }

    List<ContainerStatus> completed = amResponse.getCompletedContainersStatuses();
    if (!completed.isEmpty()) {
      handler.completed(completed);
    }
  }

  @Override
  public ContainerRequestBuilder addContainerRequest(Resource capability) {
    return addContainerRequest(capability, 1);
  }

  @Override
  public synchronized ContainerRequestBuilder addContainerRequest(Resource capability, int count) {
    return new ContainerRequestBuilder(adjustCapability(capability), count) {
      @Override
      public void apply() {
        String[] hosts = this.hosts.isEmpty() ? null : this.hosts.toArray(new String[this.hosts.size()]);
        String[] racks = this.racks.isEmpty() ? null : this.racks.toArray(new String[this.racks.size()]);

        for (int i = 0; i < count; i++) {
          AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, hosts, racks,
                                                                                priority, 1);
          amrmClient.addContainerRequest(request);
        }
      }
    };
  }

  private Resource adjustCapability(Resource resource) {
    int cores = YarnUtils.getVirtualCores(resource);
    int updatedCores = Math.max(Math.min(cores, YarnUtils.getVirtualCores(maxCapability)),
                                YarnUtils.getVirtualCores(minCapability));
    // Try and set the virtual cores, though older versions of YARN don't support this.
    // Log a message if we're on an older version and could not set it.
    if (cores != updatedCores && YarnUtils.setVirtualCores(resource, cores)) {
      LOG.info("Adjust virtual cores requirement from {} to {}.", cores, updatedCores);
    }

    int updatedMemory = Math.max(Math.min(resource.getMemory(), maxCapability.getMemory()), minCapability.getMemory());
    if (resource.getMemory() != updatedMemory) {
      LOG.info("Adjust memory requirement from {} to {} MB.", resource.getMemory(), updatedMemory);
      resource.setMemory(updatedMemory);
    }

    return resource;
  }
}
