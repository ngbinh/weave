package com.continuuity.weave.yarn;

import com.continuuity.weave.api.ListenerAdapter;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.Threads;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DistributeShellTest extends ClusterTestBase {

  @Ignore
  @Test
  public void testDistributedShell() throws InterruptedException {
    WeaveRunner weaveRunner = getWeaveRunner();

    WeaveController controller = weaveRunner.prepare(new DistributedShell("pwd", "ls -al"))
                                            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
                                            .start();

    final CountDownLatch stopLatch = new CountDownLatch(1);
    controller.addListener(new ListenerAdapter() {
      @Override
      public void terminated() {
        stopLatch.countDown();
      }

      @Override
      public void failed(StackTraceElement[] stackTraces) {
        stopLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(stopLatch.await(10, TimeUnit.SECONDS));
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
