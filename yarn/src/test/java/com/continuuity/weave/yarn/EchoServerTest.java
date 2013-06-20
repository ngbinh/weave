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

import com.continuuity.weave.api.ListenerAdapter;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.LineReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class EchoServerTest extends ClusterTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(EchoServerTest.class);

  @Test
  public void testEchoServer() throws InterruptedException, ExecutionException, IOException, URISyntaxException {
    WeaveRunner runner = getWeaveRunner();

    WeaveController controller = runner.prepare(new EchoServer(),
                                                ResourceSpecification.Builder.with()
                                                         .setCores(1)
                                                         .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
                                                         .setInstances(2)
                                                         .build())
                                        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                        .withApplicationArguments("echo")
                                        .withArguments("EchoServer", "echo2")
                                        .start();

    final CountDownLatch running = new CountDownLatch(1);
    controller.addListener(new ListenerAdapter() {
      @Override
      public void running() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(30, TimeUnit.SECONDS));

    Iterable<Discoverable> echoServices = controller.discoverService("echo");
    Assert.assertTrue(waitForSize(echoServices, 2, 60));

    for (Discoverable discoverable : echoServices) {
      String msg = "Hello: " + discoverable.getSocketAddress();

      Socket socket = new Socket(discoverable.getSocketAddress().getAddress(),
                                 discoverable.getSocketAddress().getPort());
      try {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
        LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

        writer.println(msg);
        Assert.assertEquals(msg, reader.readLine());
      } finally {
        socket.close();
      }
    }

    controller.changeInstances("EchoServer", 3);
    Assert.assertTrue(waitForSize(echoServices, 3, 60));

    echoServices = controller.discoverService("echo2");

    controller.changeInstances("EchoServer", 1);
    Assert.assertTrue(waitForSize(echoServices, 1, 60));

    Assert.assertEquals(1, Iterables.size(runner.lookupLive()));

    for (WeaveController c : runner.lookup("EchoServer")) {
      LOG.info("Stopping application: " + c.getRunId());
      c.stop().get();
    }

    Iterable<WeaveRunner.LiveInfo> apps = runner.lookupLive();
    Assert.assertTrue(waitForSize(apps, 0, 60));

    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  private <T> boolean waitForSize(Iterable<T> iterable, int count, int limit) throws InterruptedException {
    int trial = 0;
    while (Iterables.size(iterable) != count && trial < limit) {
      TimeUnit.SECONDS.sleep(1);
      trial++;
    }
    return trial < limit;
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
