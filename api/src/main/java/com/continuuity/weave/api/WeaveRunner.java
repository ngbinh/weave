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

/**
 * This interface prepares execution of {@link WeaveRunnable} and {@link WeaveApplication}.
 */
public interface WeaveRunner {

  interface LiveInfo {
    String getApplicationName();

    Iterable<RunId> getRunIds();
  }

  /**
   * Prepares to run the given {@link WeaveRunnable} with {@link ResourceSpecification#BASIC} resource specification.
   * @param runnable The runnable to run through Weave when {@link WeavePreparer#start()} is called.
   * @return A {@link WeavePreparer} for setting up runtime options.
   */
  WeavePreparer prepare(WeaveRunnable runnable);

  /**
   * Prepares to run the given {@link WeaveRunnable} with the given resource specification.
   * @param runnable The runnable to run through Weave when {@link WeavePreparer#start()} is called.
   * @param resourceSpecification The resource specification for running the runnable.
   * @return A {@link WeavePreparer} for setting up runtime options.
   */
  WeavePreparer prepare(WeaveRunnable runnable, ResourceSpecification resourceSpecification);

  /**
   * Prepares to run the given {@link WeaveApplication} as specified by the application.
   * @param application The application to run through Weave when {@link WeavePreparer#start()} is called.
   * @return A {@link WeavePreparer} for setting up runtime options.
   */
  WeavePreparer prepare(WeaveApplication application);

  /**
   * Gets a {@link WeaveController} for the given application and runId.
   * @param applicationName Name of the application.
   * @param runId The runId of the running application.
   * @return A {@link WeaveController} to interact with the application.
   */
  WeaveController lookup(String applicationName, RunId runId);

  /**
   * Gets an {@link Iterable} of {@link WeaveController} for all running instances of the given application.
   * @param applicationName Name of the application.
   * @return A live {@link Iterable} that givens the latest set of {@link WeaveController} for all running
   *         instances of the application when {@link Iterable#iterator()} is invoked.
   */
  Iterable<WeaveController> lookup(String applicationName);

  /**
   * Gets an {@link Iterable} of {@link LiveInfo}.
   * @return A live {@link Iterable} that gives the latest set of application informations that have running instances
   *         when {@link Iterable#iterator()}} is invoked.
   */
  Iterable<LiveInfo> lookupLive();
}
