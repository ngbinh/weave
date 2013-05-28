/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
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

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.internal.yarn.ports.AMRMClient;

/**
 * Data structure to represent a container request in progress (provisioning).
 */
final class ProvisionRequest {
  private final AMRMClient.ContainerRequest request;
  private final RuntimeSpecification runtimeSpec;
  private final RunId baseRunId;

  ProvisionRequest(AMRMClient.ContainerRequest request, RuntimeSpecification runtimeSpec, RunId baseRunId) {
    this.request = request;
    this.runtimeSpec = runtimeSpec;
    this.baseRunId = baseRunId;
  }

  AMRMClient.ContainerRequest getRequest() {
    return request;
  }

  RuntimeSpecification getRuntimeSpec() {
    return runtimeSpec;
  }

  RunId getBaseRunId() {
    return baseRunId;
  }
}
