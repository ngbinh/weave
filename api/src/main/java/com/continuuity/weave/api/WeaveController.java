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
package com.continuuity.weave.api;

import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.discovery.Discoverable;

/**
 * For controller a running application.
 */
public interface WeaveController extends ServiceController {

  /**
   * Adds a {@link LogHandler} for receiving application log.
   * @param handler The handler to add.
   */
  void addLogHandler(LogHandler handler);

  /**
   * Discovers the set of {@link Discoverable} endpoints that provides service for the given service name.
   * @param serviceName Name of the service to discovery.
   * @return An {@link Iterable} that gives set of latest {@link Discoverable} every time when
   *         {@link Iterable#iterator()}} is invoked.
   */
  Iterable<Discoverable> discoverService(String serviceName);


  /**
   * Changes the number of running instances of a given runnable. This method would block until
   * the command is completely executed.
   *
   * @param runnable The name of the runnable.
   * @param newCount Number of instances for the given runnable.
   */
  void changeInstances(String runnable, int newCount);
}
