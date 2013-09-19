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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
import java.util.Map;

/**
 * Implementation of {@link com.continuuity.weave.api.ResourceReport} with some
 * additional methods for maintaining the report.
 */
public final class DefaultResourceReport implements ResourceReport {
  private final SetMultimap<String, WeaveRunResources> usedResources;
  private final WeaveRunResources appMasterResources;
  private final String applicationId;

  public DefaultResourceReport(String applicationId, WeaveRunResources masterResources) {
    this.applicationId = applicationId;
    this.appMasterResources = masterResources;
    this.usedResources = HashMultimap.create();
  }

  public DefaultResourceReport(String applicationId, WeaveRunResources masterResources,
                               Map<String, Collection<WeaveRunResources>> resources) {
    this.applicationId = applicationId;
    this.appMasterResources = masterResources;
    this.usedResources = HashMultimap.create();
    for (Map.Entry<String, Collection<WeaveRunResources>> entry : resources.entrySet()) {
      this.usedResources.putAll(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Add resources used by an instance of the runnable.
   *
   * @param runnableName name of runnable.
   * @param resources resources to add.
   */
  public void addRunResources(String runnableName, WeaveRunResources resources) {
    usedResources.put(runnableName, resources);
  }

  /**
   * Remove the resource corresponding to the given runnable and container.
   *
   * @param runnableName name of runnable.
   * @param containerId container id of the runnable.
   */
  public void removeRunnableResources(String runnableName, String containerId) {
    WeaveRunResources toRemove = null;
    // could be faster if usedResources was a Table, but that makes returning the
    // report a little more complex, and this does not need to be terribly fast.
    for (WeaveRunResources resources : usedResources.get(runnableName)) {
      if (resources.getContainerId().equals(containerId)) {
        toRemove = resources;
        break;
      }
    }
    usedResources.remove(runnableName, toRemove);
  }

  /**
   * Get all the run resources being used by all instances of the specified runnable.
   *
   * @param runnableName the runnable name.
   * @return resources being used by all instances of the runnable.
   */
  @Override
  public Collection<WeaveRunResources> getRunnableResources(String runnableName) {
    return usedResources.get(runnableName);
  }

  /**
   * Get all the run resources being used across all runnables.
   *
   * @return all run resources used by all instances of all runnables.
   */
  @Override
  public Map<String, Collection<WeaveRunResources>> getResources() {
    return Multimaps.unmodifiableSetMultimap(usedResources).asMap();
  }

  /**
   * Get the resources application master is using.
   *
   * @return resources being used by the application master.
   */
  @Override
  public WeaveRunResources getAppMasterResources() {
    return appMasterResources;
  }

  /**
   * Get the id of the application master.
   *
   * @return id of the application master.
   */
  @Override
  public String getApplicationId() {
    return applicationId;
  }
}
