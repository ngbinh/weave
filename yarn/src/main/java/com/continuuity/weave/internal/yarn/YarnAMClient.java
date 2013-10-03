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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;
import java.util.List;

/**
 * This interface provides abstraction for AM to interacts with YARN to abstract out YARN version specific
 * code, making multi-version compatibility easier.
 */
public interface YarnAMClient extends Service {

  abstract class ContainerRequestBuilder {

    protected final Resource capability;
    protected final int count;
    protected final List<String> hosts = Lists.newArrayList();
    protected final List<String> racks = Lists.newArrayList();
    protected final Priority priority = Records.newRecord(Priority.class);

    public ContainerRequestBuilder(Resource capability, int count) {
      this.capability = capability;
      this.count = count;
    }

    public ContainerRequestBuilder addHosts(String firstHost, String...moreHosts) {
      return addToList(hosts, firstHost, moreHosts);
    }

    public ContainerRequestBuilder addRacks(String firstRack, String...moreRacks) {
      return addToList(racks, firstRack, moreRacks);
    }

    public ContainerRequestBuilder setPriority(int prio) {
      priority.setPriority(prio);
      return this;
    }

    public abstract void apply();

    private <T> ContainerRequestBuilder addToList(List<T> list, T first, T...more) {
      list.add(first);
      Collections.addAll(list, more);
      return this;
    }
  }

  /**
   * Callback for allocate call.
   */
  // TODO: Move AM heartbeat logic into this interface so AM only needs to handle callback.
  interface AllocateHandler {
    void acquired(List<ProcessLauncher> launchers);

    void completed(List<ContainerStatus> completed);
  }

  void allocate(float progress, AllocateHandler handler) throws Exception;

  ContainerRequestBuilder addContainerRequest(Resource capability);

  ContainerRequestBuilder addContainerRequest(Resource capability, int count);
}
