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
package com.continuuity.weave.internal;

import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;

/**
 * A {@link Service} that wrap a {@link ZKClientService} and a {@link Service} that
 * guarantee the ZKClientService will starts before the Service and stopped after
 * the Service is stopped.
 */
public final class ZKServiceWrapper extends AbstractService {

  private final ZKClientService zkClientService;
  private final Service delegateService;
  private volatile Throwable failureCause;

  public ZKServiceWrapper(ZKClientService zkClientService, Service delegateService) {
    this.zkClientService = zkClientService;
    this.delegateService = delegateService;

    // Listen to state change of the delgateService
    this.delegateService.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        // When the delegate service is running, then this service is consider started.
        notifyStarted();
      }

      @Override
      public void stopping(State from) {
        // If stop is triggered, stop the wrapper service. This is mainly to trigger external listeners
        // The AbstractService state lock would guaranteed that it won't be recursive.
        ZKServiceWrapper.this.stop();
      }

      @Override
      public void terminated(State from) {
        // Stop the zkClient when the delegate service is completed.
        ZKServiceWrapper.this.zkClientService.stop();
      }

      @Override
      public void failed(State from, Throwable failure) {
        // Stop the zkClient when the delegate service is done because of failure
        // Also memorize the failure reason to notifyFailed when stopping of zkClient is done.
        failureCause = failure;
        ZKServiceWrapper.this.zkClientService.stop();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Listen to state change of zkClient
    this.zkClientService.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        // When ZKClient start, starts the delegate service as well.
        ZKServiceWrapper.this.delegateService.start();
      }

      @Override
      public void terminated(State from) {
        // When ZKClient stopped, notify service completed based on the result of termination of the delegate service.
        if (failureCause == null) {
          notifyStopped();
        } else {
          notifyFailed(failureCause);
        }
      }

      @Override
      public void failed(State from, Throwable failure) {
        // Same logic as terminate.
        terminated(from);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  @Override
  protected void doStart() {
    zkClientService.start();
  }

  @Override
  protected void doStop() {
    delegateService.stop();
  }
}
