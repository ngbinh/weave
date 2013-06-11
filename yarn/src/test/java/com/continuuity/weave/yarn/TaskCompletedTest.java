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
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.ListenerAdapter;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.Threads;
import com.google.common.base.Throwables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Testing application master will shutdown itself when all tasks are completed.
 */
public class TaskCompletedTest extends ClusterTestBase {

  public static final class SleepTask extends AbstractWeaveRunnable {

    @Override
    public void run() {
      // Randomly sleep for 3-5 seconds.
      try {
        TimeUnit.SECONDS.sleep(new Random().nextInt(3) + 3);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void stop() {
      // No-op
    }
  }

  @Test
  public void testTaskCompleted() throws InterruptedException {
    WeaveRunner weaveRunner = getWeaveRunner();
    WeaveController controller = weaveRunner.prepare(new SleepTask(),
                                                ResourceSpecification.Builder.with()
                                                  .setCores(1)
                                                  .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                                                  .setInstances(3).build())
                                            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                            .start();

    final CountDownLatch runLatch = new CountDownLatch(1);
    final CountDownLatch stopLatch = new CountDownLatch(1);
    controller.addListener(new ListenerAdapter() {

      @Override
      public void running() {
        runLatch.countDown();
      }

      @Override
      public void terminated() {
        stopLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(runLatch.await(1, TimeUnit.MINUTES));

    Assert.assertTrue(stopLatch.await(1, TimeUnit.MINUTES));

    TimeUnit.SECONDS.sleep(2);
  }

  @Before
  public void init() throws IOException {
    doInit();
  }

  @After
  public void finish() {
    doFinish();
  }
}
