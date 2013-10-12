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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class Hadoop20YarnLaunchContext implements YarnLaunchContext {

  private static final Function<YarnLocalResource, LocalResource> RESOURCE_TRANSFORM;

  static {
    // Creates transform function from YarnLocalResource -> LocalResource
    RESOURCE_TRANSFORM = new Function<YarnLocalResource, LocalResource>() {
      @Override
      public LocalResource apply(YarnLocalResource input) {
        return input.getLocalResource();
      }
    };
  }

  private final ContainerLaunchContext launchContext;

  public Hadoop20YarnLaunchContext() {
    launchContext = Records.newRecord(ContainerLaunchContext.class);
  }

  @Override
  public <T> T getLaunchContext() {
    return (T) launchContext;
  }

  @Override
  public void setCredentials(Credentials credentials) {
    try {
      DataOutputBuffer buffer = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(buffer);
      launchContext.setContainerTokens(ByteBuffer.wrap(buffer.getData(), 0, buffer.getLength()));
    } catch (IOException e) {
      // Exception shouldn't be thrown as it's writing to memory buffer.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void setLocalResources(Map<String, YarnLocalResource> localResources) {
    launchContext.setLocalResources(Maps.transformValues(localResources, RESOURCE_TRANSFORM));
  }

  @Override
  public void setServiceData(Map<String, ByteBuffer> serviceData) {
    launchContext.setServiceData(serviceData);
  }

  @Override
  public Map<String, String> getEnvironment() {
    return launchContext.getEnvironment();
  }

  @Override
  public void setEnvironment(Map<String, String> environment) {
    launchContext.setEnvironment(environment);
  }

  @Override
  public List<String> getCommands() {
    return launchContext.getCommands();
  }

  @Override
  public void setCommands(List<String> commands) {
    launchContext.setCommands(commands);
  }

  @Override
  public void setApplicationACLs(Map<ApplicationAccessType, String> acls) {
    launchContext.setApplicationACLs(acls);
  }
}
