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
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.LineReader;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
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
            .setCores(1)
            .setMemory(128, ResourceSpecification.SizeUnit.MEGA)
            .setInstances(2).build()).noLocalFiles()
          .add("echo2", new EchoServer(), ResourceSpecification.Builder.with()
            .setCores(2)
            .setMemory(256, ResourceSpecification.SizeUnit.MEGA)
            .setInstances(1).build()).noLocalFiles()
        .anyOrder()
        .build();
    }
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
    Assert.assertTrue(waitForSize(echoServices, 3, 60));
    ResourceReport report = controller.getResourceReport();
    // make sure resources for echo1 and echo2 are there
    Map<String, Set<WeaveRunResources>> usedResources = report.getResources();
    Assert.assertEquals(2, usedResources.keySet().size());
    Assert.assertTrue(usedResources.containsKey("echo1"));
    Assert.assertTrue(usedResources.containsKey("echo2"));

    Set<WeaveRunResources> echo1Resources = usedResources.get("echo1");
    // 2 instances of echo1
    Assert.assertEquals(2, echo1Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (WeaveRunResources resources : echo1Resources) {
      Assert.assertEquals(128, resources.getMemoryMB());
    }

    Set<WeaveRunResources> echo2Resources = usedResources.get("echo2");
    // 2 instances of echo1
    Assert.assertEquals(1, echo2Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (WeaveRunResources resources : echo2Resources) {
      Assert.assertEquals(256, resources.getMemoryMB());
    }

    // Decrease number of instances of echo1 from 2 to 1
    controller.changeInstances("echo1", 1);
    echoServices = controller.discoverService("echo1");
    Assert.assertTrue(waitForSize(echoServices, 1, 60));
    report = controller.getResourceReport();

    // make sure resources for echo1 and echo2 are there
    usedResources = report.getResources();
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

  private <T> boolean waitForSize(Iterable<T> iterable, int count, int limit) throws InterruptedException {
    int trial = 0;
    int size = Iterables.size(iterable);
    while (size != count && trial < limit) {
      LOG.info("Waiting for {} size {} == {}", iterable, size, count);
      TimeUnit.SECONDS.sleep(1);
      trial++;
      size = Iterables.size(iterable);
    }
    return trial < limit;
  }
}
