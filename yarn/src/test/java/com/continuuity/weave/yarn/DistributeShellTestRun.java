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
public class DistributeShellTestRun {

  @Ignore
  @Test
  public void testDistributedShell() throws InterruptedException {
    WeaveRunner weaveRunner = YarnTestSuite.getWeaveRunner();

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
