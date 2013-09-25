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

import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

/**
 * A {@link Service} that wrap a {@link ZKClientService} and a {@link Service} that
 * guarantee the ZKClientService will starts before the Service and stopped after
 * the Service is stopped.
 */
public final class ZKServiceWrapper extends AbstractIdleService {

  // It is important that this class extends from AbstractIdleService so that start() and stop()
  // happens in its own thread so that in the listener added to the delegate service, when
  // performing ZKServiceWrapper.this.stop(), it won't creates a deadlock.

  private final ZKClientService zkClientService;
  private final Service delegateService;

  public ZKServiceWrapper(ZKClientService zkClientService, Service delegateService) {
    this.zkClientService = zkClientService;
    this.delegateService = delegateService;

    // Listen to state change of the delgateService
    this.delegateService.addListener(new ServiceListenerAdapter() {

      @Override
      public void stopping(State from) {
        // If stop is triggered, stop the wrapper service. This is mainly to trigger external listeners
        // The AbstractService state lock would guaranteed that it won't be recursive.
        ZKServiceWrapper.this.stop();
      }

      @Override
      public void failed(State from, Throwable failure) {
        ZKServiceWrapper.this.stop();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  @Override
  protected void startUp() throws Exception {
    for (ListenableFuture<State> future : Services.chainStart(zkClientService, delegateService).get()) {
      future.get();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    for (ListenableFuture<State> future : Services.chainStop(delegateService, zkClientService).get()) {
      future.get();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("ZKClient", zkClientService.getConnectString())
                  .add("Service", delegateService.getClass().getName())
                  .toString();
  }
}
