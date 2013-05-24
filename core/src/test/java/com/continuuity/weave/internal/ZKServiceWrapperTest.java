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
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ZKServiceWrapperTest {

  private static InMemoryZKServer zkServer;

  @Test
  public void testExternalControl() {
    ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        // No-op
      }

      @Override
      protected void shutDown() throws Exception {
        // No-op
      }
    };

    Service wrapperService = new ZKServiceWrapper(zkClientService, service);

    wrapperService.startAndWait();
    Assert.assertTrue(zkClientService.isRunning());
    Assert.assertTrue(service.isRunning());
    Assert.assertTrue(wrapperService.isRunning());

    wrapperService.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, zkClientService.state());
    Assert.assertEquals(Service.State.TERMINATED, service.state());
    Assert.assertEquals(Service.State.TERMINATED, wrapperService.state());
  }

  @Test
  public void testServiceLifecycle() throws InterruptedException {
    ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        // No-op
      }

      @Override
      protected void shutDown() throws Exception {
        // No-op
      }
    };

    Service wrapperService = new ZKServiceWrapper(zkClientService, service);

    wrapperService.startAndWait();
    Assert.assertTrue(zkClientService.isRunning());
    Assert.assertTrue(service.isRunning());
    Assert.assertTrue(wrapperService.isRunning());

    // Stop the service, the wrapperService state should changed as well.
    final CountDownLatch stopLatch = new CountDownLatch(1);
    wrapperService.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        stopLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);


    service.stopAndWait();
    Assert.assertTrue(stopLatch.await(2, TimeUnit.SECONDS));
  }

  @Test
  public void testServiceFailure() throws InterruptedException {
    ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();

    final CountDownLatch exceptionLatch = new CountDownLatch(1);
    Service service = new AbstractExecutionThreadService() {
      @Override
      protected void run() throws Exception {
        exceptionLatch.await();
        throw new IllegalStateException("Exception from run");
      }
    };

    Service wrapperService = new ZKServiceWrapper(zkClientService, service);

    wrapperService.startAndWait();
    Assert.assertTrue(zkClientService.isRunning());
    Assert.assertTrue(service.isRunning());
    Assert.assertTrue(wrapperService.isRunning());

    final CountDownLatch failureLatch = new CountDownLatch(1);
    wrapperService.addListener(new ServiceListenerAdapter() {
      @Override
      public void failed(Service.State from, Throwable failure) {
        failureLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    exceptionLatch.countDown();
    Assert.assertTrue(failureLatch.await(2, TimeUnit.SECONDS));
  }

  @BeforeClass
  public static void init() {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }
}