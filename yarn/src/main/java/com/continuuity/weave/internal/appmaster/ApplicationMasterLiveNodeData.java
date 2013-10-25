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

/**
 * Represents data being stored in the live node of the application master.
 */
public final class ApplicationMasterLiveNodeData {

  private final int appId;
  private final long appIdClusterTime;
  private final String containerId;

  public ApplicationMasterLiveNodeData(int appId, long appIdClusterTime, String containerId) {
    this.appId = appId;
    this.appIdClusterTime = appIdClusterTime;
    this.containerId = containerId;
  }

  public int getAppId() {
    return appId;
  }

  public long getAppIdClusterTime() {
    return appIdClusterTime;
  }

  public String getContainerId() {
    return containerId;
  }
}
