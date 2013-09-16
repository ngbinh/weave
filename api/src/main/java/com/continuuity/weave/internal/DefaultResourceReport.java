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

import com.continuuity.weave.api.ResourceReport;
import com.continuuity.weave.api.WeaveRunResources;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 * Straightforward implementation of {@link com.continuuity.weave.api.ResourceReport} with some
 * additional methods for maintaining the report.
 */
public final class DefaultResourceReport implements ResourceReport {
  private final Map<String, Set<WeaveRunResources>> usedResources;
  private final WeaveRunResources appMasterResources;
  private final String applicationId;

  public DefaultResourceReport(String applicationId, WeaveRunResources masterResources) {
    this.applicationId = applicationId;
    this.appMasterResources = masterResources;
    this.usedResources = Maps.newHashMap();
  }

  public DefaultResourceReport(String applicationId, WeaveRunResources masterResources,
                               Map<String, Set<WeaveRunResources>> resources) {
    this.applicationId = applicationId;
    this.appMasterResources = masterResources;
    this.usedResources = resources;
  }

  public void addRunResource(String runnableName, WeaveRunResources resources) {
    if (!usedResources.containsKey(runnableName)) {
      usedResources.put(runnableName, Sets.<WeaveRunResources>newTreeSet());
    }
    this.usedResources.get(runnableName).add(resources);
  }

  public void removeRunnableContext(String runnableName, int instanceId) {
    Set<WeaveRunResources> runnableResources = usedResources.get(runnableName);
    WeaveRunResources toRemove = null;
    for (WeaveRunResources resources : runnableResources) {
      if (resources.getInstanceId() == instanceId) {
        toRemove = resources;
        break;
      }
    }
    runnableResources.remove(toRemove);
  }

  @Override
  public Set<WeaveRunResources> getResourcesForRunnable(String runnableName) {
    return usedResources.get(runnableName);
  }

  @Override
  public Map<String, Set<WeaveRunResources>> getResources() {
    return usedResources;
  }

  @Override
  public WeaveRunResources getAppMasterResources() {
    return appMasterResources;
  }

  public String getApplicationId() {
    return applicationId;
  }
}
