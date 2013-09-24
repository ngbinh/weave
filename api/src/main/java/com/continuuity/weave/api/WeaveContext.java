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
package com.continuuity.weave.api;

import java.net.InetAddress;

/**
 * Represents the runtime context of a {@link WeaveRunnable}.
 */
public interface WeaveContext extends ServiceAnnouncer {

  /**
   * Returns the {@link RunId} of this running instance of {@link WeaveRunnable}.
   */
  RunId getRunId();

  /**
   * Returns the {@link RunId} of this running application.
   */
  RunId getApplicationRunId();

  /**
   * Returns the number of running instances assigned for this {@link WeaveRunnable}.
   */
  int getInstanceCount();

  /**
   * Returns the hostname that the runnable is running on.
   */
  InetAddress getHost();

  /**
   * Returns the runtime arguments that are passed to the {@link WeaveRunnable}.
   */
  String[] getArguments();

  /**
   * Returns the runtime arguments that are passed to the {@link WeaveApplication}.
   */
  String[] getApplicationArguments();

  /**
   * Returns the {@link WeaveRunnableSpecification} that was created by {@link WeaveRunnable#configure()}.
   */
  WeaveRunnableSpecification getSpecification();

  /**
   * Returns an integer id from 0 to (instanceCount - 1).
   */
  int getInstanceId();

  /**
   * Returns the number of virtual cores the runnable is allowed to use.
   */
  int getVirtualCores();

  /**
   * Returns the amount of memory in MB the runnable is allowed to use.
   */
  int getMaxMemoryMB();
}
