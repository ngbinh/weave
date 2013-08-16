/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
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
