/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal;

/**
 *
 */
public final class ContainerLiveNodeData {

  private final String containerId;
  private final String host;

  public ContainerLiveNodeData(String containerId, String host) {
    this.containerId = containerId;
    this.host = host;
  }

  public String getContainerId() {
    return containerId;
  }

  public String getHost() {
    return host;
  }
}
