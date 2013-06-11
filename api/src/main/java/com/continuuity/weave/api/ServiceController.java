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

import com.continuuity.weave.common.Cancellable;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Executor;

/**
 * This interface is for controlling a running application.
 */
public interface ServiceController {

  /**
   * Returns the {@link RunId} of the running application.
   */
  RunId getRunId();

  /**
   * Sends a user command to the running application.
   * @param command The command to send.
   * @return A {@link ListenableFuture} that will be completed when the command is successfully processed
   *         by the target application.
   */
  ListenableFuture<Command> sendCommand(Command command);

  /**
   * Sends a user command to the given runnable of the running application.
   * @param runnableName Name of the {@link WeaveRunnable}.
   * @param command The command to send.
   * @return A {@link ListenableFuture} that will be completed when the command is successfully processed
   *         by the target runnable.
   */
  ListenableFuture<Command> sendCommand(String runnableName, Command command);

  /**
   * Returns the current state of the running application that this controller is connected to.
   */
  State getState();

  /**
   * Requests to stop a running service.
   * @return A {@link ListenableFuture} that will be completed when the service is stopped.
   */
  ListenableFuture<State> stop();

  /**
   * Requests to stop a running service and wait for its completion.
   */
  void stopAndWait();

  /**
   * Requests to forcefully kills a running service.
   */
  void kill();

  /**
   * Adds a listener to watch for state changes of the service.
   * @param listener The listener to attach.
   * @param executor An executor that executes calls to the given listener.
   * @return A {@link Cancellable} for removing the listener.
   */
  Cancellable addListener(Listener listener, Executor executor);

  /**
   * Represents the service state.
   */
  enum State {

    /**
     * A service state is not known.
     */
    UNKNOWN,

    /**
     * A service in this state is transitioning to {@link #RUNNING}.
     */
    STARTING,

    /**
     * A service in this state is operational.
     */
    RUNNING,

    /**
     * A service in this state is transitioning to {@link #TERMINATED}.
     */
    STOPPING,

    /**
     * A service in this state has completed execution normally. It does minimal work and consumes
     * minimal resources.
     */
    TERMINATED,

    /**
     * A service in this state has encountered a problem and may not be operational. It cannot be
     * started nor stopped.
     */
    FAILED
  }

  /**
   * Listener for listening to service state changes.
   */
  interface Listener {

    void starting();

    void running();

    void stopping();

    void terminated();

    void failed(StackTraceElement[] stackTraces);
  }
}
