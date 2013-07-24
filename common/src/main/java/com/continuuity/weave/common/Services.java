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
package com.continuuity.weave.common;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility methods for help dealing with {@link Service}.
 */
public final class Services {

  /**
   * Starts a list of {@link Service} one by one. Starting of next Service is triggered from the callback listener
   * thread of the previous Service.
   *
   * @param firstService First service to start.
   * @param moreServices The rest services to start.
   * @return A {@link ListenableFuture} that will be completed when all services are started, with the
   *         result carries the completed {@link ListenableFuture} of each corresponding service in the
   *         same order as they are passed to this method.
   */
  public static ListenableFuture<List<ListenableFuture<Service.State>>> chainStart(Service firstService,
                                                                                   Service...moreServices) {
    return doChain(true, firstService, moreServices);
  }

  /**
   * Stops a list of {@link Service} one by one. It behaves the same as
   * {@link #chainStart(com.google.common.util.concurrent.Service, com.google.common.util.concurrent.Service...)}
   * except {@link com.google.common.util.concurrent.Service#stop()} is called instead of start.
   *
   * @param firstService First service to stop.
   * @param moreServices The rest services to stop.
   * @return A {@link ListenableFuture} that will be completed when all services are stopped.
   * @see #chainStart(com.google.common.util.concurrent.Service, com.google.common.util.concurrent.Service...)
   */
  public static ListenableFuture<List<ListenableFuture<Service.State>>> chainStop(Service firstService,
                                                                                  Service...moreServices) {
    return doChain(false, firstService, moreServices);
  }

  /**
   * Performs the actual logic of chain Service start/stop.
   */
  private static ListenableFuture<List<ListenableFuture<Service.State>>> doChain(boolean doStart,
                                                                                 Service firstService,
                                                                                 Service...moreServices) {
    SettableFuture<List<ListenableFuture<Service.State>>> resultFuture = SettableFuture.create();
    List<ListenableFuture<Service.State>> result = Lists.newArrayListWithCapacity(moreServices.length + 1);

    ListenableFuture<Service.State> future = doStart ? firstService.start() : firstService.stop();
    future.addListener(createChainListener(future, moreServices, new AtomicInteger(0), result, resultFuture, doStart),
                       Threads.SAME_THREAD_EXECUTOR);
    return resultFuture;
  }

  /**
   * Returns a {@link Runnable} that can be used as a {@link ListenableFuture} listener to trigger
   * further service action or completing the result future. Used by
   * {@link #doChain(boolean, com.google.common.util.concurrent.Service, com.google.common.util.concurrent.Service...)}
   */
  private static Runnable createChainListener(final ListenableFuture<Service.State> future, final Service[] services,
                                              final AtomicInteger idx,
                                              final List<ListenableFuture<Service.State>> result,
                                              final SettableFuture<List<ListenableFuture<Service.State>>> resultFuture,
                                              final boolean doStart) {
    return new Runnable() {

      @Override
      public void run() {
        result.add(future);
        int nextIdx = idx.getAndIncrement();
        if (nextIdx == services.length) {
          resultFuture.set(result);
          return;
        }
        ListenableFuture<Service.State> actionFuture = doStart ? services[nextIdx].start() : services[nextIdx].stop();
        actionFuture.addListener(createChainListener(actionFuture, services, idx, result, resultFuture, doStart),
                                 Threads.SAME_THREAD_EXECUTOR);
      }
    };
  }

  private Services() {
  }
}
