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
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Threads;
import com.google.common.util.concurrent.Service;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This test is executed by {@link YarnTestSuite}.
 */
public final class DistributeShellTestRun extends BaseYarnTest {

  @Ignore
  @Test
  public void testDistributedShell() throws InterruptedException {
    WeaveRunner weaveRunner = YarnTestUtils.getWeaveRunner();

    WeaveController controller = weaveRunner.prepare(new DistributedShell("pwd", "ls -al"))
                                            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
                                            .start();

    final CountDownLatch stopLatch = new CountDownLatch(1);
    controller.addListener(new ServiceListenerAdapter() {

      @Override
      public void terminated(Service.State from) {
        stopLatch.countDown();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        stopLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(stopLatch.await(10, TimeUnit.SECONDS));
  }
}
