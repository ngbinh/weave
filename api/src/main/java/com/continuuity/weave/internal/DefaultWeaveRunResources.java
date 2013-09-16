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
package com.continuuity.weave.internal;

import com.continuuity.weave.api.WeaveRunResources;

/**
 *  Straightforward implementation of {@link com.continuuity.weave.api.WeaveRunResources}.
 */
public class DefaultWeaveRunResources implements WeaveRunResources {
  private final String containerId;
  private final int instanceId;
  private final int virtualCores;
  private final int memoryMB;
  private final String host;

  public DefaultWeaveRunResources(int instanceId, String containerId,
                                  int cores, int memoryMB, String host) {
    this.instanceId = instanceId;
    this.containerId = containerId;
    this.virtualCores = cores;
    this.memoryMB = memoryMB;
    this.host = host;
  }

  /**
   * @return instance id of the runnable.
   */
  @Override
  public int getInstanceId() {
    return instanceId;
  }

  /**
   * @return id of the container the runnable is running in.
   */
  @Override
  public String getContainerId() {
    return containerId;
  }

  /**
   * @return number of cores the runnable is allowed to use.  YARN must be at least v2.1.0 and
   *   it must be configured to use cgroups in order for this to be a reflection of truth.
   */
  @Override
  public int getVirtualCores() {
    return virtualCores;
  }

  /**
   * @return amount of memory in MB the runnable is allowed to use.
   */
  @Override
  public int getMemoryMB() {
    return memoryMB;
  }

  /**
   * @return the host the runnable is running on.
   */
  @Override
  public String getHost() {
    return host;
  }

  /**
   * Ordered first by instance id, then host, then container id, then memory, then cores.
   * @param o object to compare this one to.
   * @return negative, 0, or positive if this object is less than, equal to, or greater than the
   *   object passed in.
   */
  @Override
  public int compareTo(Object o) {
    WeaveRunResources other = (WeaveRunResources) o;
    int compare = compareInts(instanceId, other.getInstanceId());
    if (compare != 0) return compare;
    compare = this.getHost().compareTo(other.getHost());
    if (compare != 0) return compare;
    compare = containerId.compareTo(other.getContainerId());
    if (compare != 0) return compare;
    compare = compareInts(memoryMB, other.getMemoryMB());
    if (compare != 0) return compare;
    return compareInts(virtualCores, other.getVirtualCores());
  }

  private int compareInts(int a, int b) {
    if (a == b) return 0;
    return (a < b) ? -1 : 1;
  }
}
