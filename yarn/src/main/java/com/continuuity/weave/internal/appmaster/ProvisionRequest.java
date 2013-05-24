/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.internal.RunIds;
import com.continuuity.weave.internal.yarn.ports.AMRMClient;

/**
*
*/
final class ProvisionRequest {
  private final AMRMClient.ContainerRequest request;
  private final RuntimeSpecification runtimeSpec;
  private final RunId baseRunId;

  ProvisionRequest(AMRMClient.ContainerRequest request, RuntimeSpecification runtimeSpec) {
    this.request = request;
    this.runtimeSpec = runtimeSpec;
    this.baseRunId = RunIds.generate();
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
