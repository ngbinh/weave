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

import com.continuuity.weave.internal.ContainerInfo;
import com.continuuity.weave.internal.ProcessLauncher;
import com.continuuity.weave.internal.appmaster.RunnableProcessLauncher;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;

/**
 *
 */
public final class Hadoop21YarnAMClient extends AbstractIdleService implements YarnAMClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop21YarnAMClient.class);

  private final ContainerId containerId;
  private final AMRMClient<AMRMClient.ContainerRequest> amrmClient;
  private final Hadoop21YarnNMClient nmClient;
  private InetSocketAddress trackerAddr;
  private URL trackerUrl;
  private Resource maxCapability;

  public Hadoop21YarnAMClient(Configuration conf) {
    String masterContainerId = System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.name());
    Preconditions.checkArgument(masterContainerId != null,
                                "Missing %s from environment", ApplicationConstants.Environment.CONTAINER_ID.name());
    this.containerId = ConverterUtils.toContainerId(masterContainerId);

    this.amrmClient = AMRMClient.createAMRMClient();
    this.amrmClient.init(conf);
    this.nmClient = new Hadoop21YarnNMClient(conf);
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkNotNull(trackerAddr, "Tracker address not set.");
    Preconditions.checkNotNull(trackerUrl, "Tracker URL not set.");

    amrmClient.start();
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster(trackerAddr.getHostName(),
                                                                                      trackerAddr.getPort(),
                                                                                      trackerUrl.toString());
    maxCapability = response.getMaximumResourceCapability();
    nmClient.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    nmClient.stopAndWait();
    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, trackerUrl.toString());
    amrmClient.stop();
  }

  @Override
  public ContainerId getContainerId() {
    return containerId;
  }

  @Override
  public String getHost() {
    return System.getenv().get(ApplicationConstants.Environment.NM_HOST.name());
  }

  @Override
  public void setTracker(InetSocketAddress trackerAddr, URL trackerUrl) {
    this.trackerAddr = trackerAddr;
    this.trackerUrl = trackerUrl;
  }

  @Override
  public synchronized void allocate(float progress, AllocateHandler handler) throws Exception {
    AllocateResponse allocateResponse = amrmClient.allocate(progress);
    List<ProcessLauncher<ContainerInfo>> launchers
      = Lists.newArrayListWithCapacity(allocateResponse.getAllocatedContainers().size());

    for (Container container : allocateResponse.getAllocatedContainers()) {
      launchers.add(new RunnableProcessLauncher(container, nmClient));
    }

    if (!launchers.isEmpty()) {
      handler.acquired(launchers);

      // If no process has been launched through the given launcher, return the container.
      for (ProcessLauncher<ContainerInfo> l : launchers) {
        // This cast always works.
        RunnableProcessLauncher launcher = (RunnableProcessLauncher) l;
        if (!launcher.isLaunched()) {
          Container container = launcher.getContainer();
          LOG.info("Nothing to run in container, releasing it: {}", container);
          amrmClient.releaseAssignedContainer(container.getId());
        }
      }
    }

    List<ContainerStatus> completed = allocateResponse.getCompletedContainersStatuses();
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
          AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, hosts, racks, priority);
          amrmClient.addContainerRequest(request);
        }
      }
    };
  }

  private Resource adjustCapability(Resource resource) {
    int cores = resource.getVirtualCores();
    int updatedCores = Math.min(resource.getVirtualCores(), maxCapability.getVirtualCores());

    if (cores != updatedCores) {
      resource.setVirtualCores(updatedCores);
      LOG.info("Adjust virtual cores requirement from {} to {}.", cores, updatedCores);
    }

    int updatedMemory = Math.min(resource.getMemory(), maxCapability.getMemory());
    if (resource.getMemory() != updatedMemory) {
      resource.setMemory(updatedMemory);
      LOG.info("Adjust memory requirement from {} to {} MB.", resource.getMemory(), updatedMemory);
    }

    return resource;
  }
}
