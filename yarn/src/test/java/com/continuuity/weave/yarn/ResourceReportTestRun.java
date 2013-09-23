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

import com.continuuity.weave.api.ResourceReport;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunResources;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Using echo server to test resource reports.
 * This test is executed by {@link com.continuuity.weave.yarn.YarnTestSuite}.
 */
public class ResourceReportTestRun {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceReportTestRun.class);

  private class ResourceApplication implements WeaveApplication {
    @Override
    public WeaveSpecification configure() {
      return WeaveSpecification.Builder.with()
        .setName("ResourceApplication")
        .withRunnable()
          .add("echo1", new EchoServer(), ResourceSpecification.Builder.with()
            .setVirtualCores(1)
            .setMemory(128, ResourceSpecification.SizeUnit.MEGA)
            .setInstances(2).build()).noLocalFiles()
          .add("echo2", new EchoServer(), ResourceSpecification.Builder.with()
            .setVirtualCores(2)
            .setMemory(256, ResourceSpecification.SizeUnit.MEGA)
            .setInstances(1).build()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  @Test
  public void testResourceReportWithFailingContainers() throws InterruptedException, IOException,
    TimeoutException, ExecutionException {
    WeaveRunner runner = YarnTestSuite.getWeaveRunner();

    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(128, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(2)
      .build();
    WeaveController controller = runner.prepare(new BuggyServer(), resourceSpec)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("echo")
      .withArguments("BuggyServer", "echo2")
      .start();

    final CountDownLatch running = new CountDownLatch(1);
    controller.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(30, TimeUnit.SECONDS));

    Iterable<Discoverable> echoServices = controller.discoverService("echo");
    Assert.assertTrue(YarnTestSuite.waitForSize(echoServices, 2, 60));
    // check that we have 2 runnables.
    ResourceReport report = controller.getResourceReport();
    Assert.assertEquals(2, report.getRunnableResources("BuggyServer").size());

    // cause a divide by 0 in one server
    Discoverable discoverable = echoServices.iterator().next();
    Socket socket = new Socket(discoverable.getSocketAddress().getAddress(),
                               discoverable.getSocketAddress().getPort());
    try {
      PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
      writer.println("0");
    } finally {
      socket.close();
    }

    // takes some time for app master to find out the container completed...
    TimeUnit.SECONDS.sleep(5);
    // check that we have 1 runnable, not 2.
    report = controller.getResourceReport();
    Assert.assertEquals(1, report.getRunnableResources("BuggyServer").size());

    controller.stop().get(10, TimeUnit.SECONDS);
    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testResourceReport() throws InterruptedException, ExecutionException, IOException,
    URISyntaxException, TimeoutException {
    WeaveRunner runner = YarnTestSuite.getWeaveRunner();

    WeaveController controller = runner.prepare(new ResourceApplication())
                                        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                        .withApplicationArguments("echo")
                                        .withArguments("echo1", "echo1")
                                        .withArguments("echo2", "echo2")
                                        .start();

    final CountDownLatch running = new CountDownLatch(1);
    controller.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(30, TimeUnit.SECONDS));

    // wait for 3 echo servers to come up
    Iterable<Discoverable> echoServices = controller.discoverService("echo");
    Assert.assertTrue(YarnTestSuite.waitForSize(echoServices, 3, 60));
    ResourceReport report = controller.getResourceReport();
    // make sure resources for echo1 and echo2 are there
    Map<String, Collection<WeaveRunResources>> usedResources = report.getResources();
    Assert.assertEquals(2, usedResources.keySet().size());
    Assert.assertTrue(usedResources.containsKey("echo1"));
    Assert.assertTrue(usedResources.containsKey("echo2"));

    Collection<WeaveRunResources> echo1Resources = usedResources.get("echo1");
    // 2 instances of echo1
    Assert.assertEquals(2, echo1Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (WeaveRunResources resources : echo1Resources) {
      Assert.assertEquals(128, resources.getMemoryMB());
    }

    Collection<WeaveRunResources > echo2Resources = usedResources.get("echo2");
    // 2 instances of echo1
    Assert.assertEquals(1, echo2Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (WeaveRunResources resources : echo2Resources) {
      Assert.assertEquals(256, resources.getMemoryMB());
    }

    // Decrease number of instances of echo1 from 2 to 1
    controller.changeInstances("echo1", 1);
    echoServices = controller.discoverService("echo1");
    Assert.assertTrue(YarnTestSuite.waitForSize(echoServices, 1, 60));
    report = controller.getResourceReport();

    // make sure resources for echo1 and echo2 are there
    usedResources = report.getResources();
    Assert.assertEquals(2, usedResources.keySet().size());
    Assert.assertTrue(usedResources.containsKey("echo1"));
    Assert.assertTrue(usedResources.containsKey("echo2"));

    echo1Resources = usedResources.get("echo1");
    // 1 instance of echo1 now
    Assert.assertEquals(1, echo1Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (WeaveRunResources resources : echo1Resources) {
      Assert.assertEquals(128, resources.getMemoryMB());
    }

    echo2Resources = usedResources.get("echo2");
    // 2 instances of echo1
    Assert.assertEquals(1, echo2Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (WeaveRunResources resources : echo2Resources) {
      Assert.assertEquals(256, resources.getMemoryMB());
    }

    controller.stop().get(10, TimeUnit.SECONDS);
    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }
}
