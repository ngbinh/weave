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

import com.continuuity.weave.internal.ContainerInfo;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.google.common.base.Throwables;
import org.apache.hadoop.yarn.api.records.Container;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *
 */
public final class YarnContainerInfo implements ContainerInfo {

  private final String id;
  private final InetAddress host;
  private final int port;
  private final int virtualCores;
  private final int memoryMB;

  public YarnContainerInfo(Container container) {
    try {
      this.id = container.getId().toString();
      this.port = container.getNodeId().getPort();
      this.virtualCores = YarnUtils.getVirtualCores(container.getResource());
      this.memoryMB = container.getResource().getMemory();
      this.host = InetAddress.getByName(container.getNodeId().getHost());
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public InetAddress getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public int getMemoryMB() {
    return memoryMB;
  }

  @Override
  public int getVirtualCores() {
    return virtualCores;
  }
}
