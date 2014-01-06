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

import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.LineReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class FailureRestartTestRun extends BaseYarnTest {

  @Test
  public void testFailureRestart() throws Exception {
    WeaveRunner runner = YarnTestUtils.getWeaveRunner();

    ResourceSpecification resource = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(2)
      .build();
    WeaveController controller = runner.prepare(new FailureRunnable(), resource)
      .withApplicationArguments("failure")
      .withArguments(FailureRunnable.class.getSimpleName(), "failure2")
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    Iterable<Discoverable> discoverables = controller.discoverService("failure");
    Assert.assertTrue(YarnTestUtils.waitForSize(discoverables, 2, 60));

    // Make sure we see the right instance IDs
    Assert.assertEquals(Sets.newHashSet(0, 1), getInstances(discoverables));

    // Kill server with instanceId = 0
    controller.sendCommand(FailureRunnable.class.getSimpleName(), Command.Builder.of("kill0").build());

    // Do a shot sleep, make sure the runnable is killed.
    TimeUnit.SECONDS.sleep(5);

    Assert.assertTrue(YarnTestUtils.waitForSize(discoverables, 2, 60));
    // Make sure we see the right instance IDs
    Assert.assertEquals(Sets.newHashSet(0, 1), getInstances(discoverables));

    controller.stopAndWait();
  }

  private Set<Integer> getInstances(Iterable<Discoverable> discoverables) throws IOException {
    Set<Integer> instances = Sets.newHashSet();
    for (Discoverable discoverable : discoverables) {
      InetSocketAddress socketAddress = discoverable.getSocketAddress();
      Socket socket = new Socket(socketAddress.getAddress(), socketAddress.getPort());
      try {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
        LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

        String msg = "Failure";
        writer.println(msg);

        String line = reader.readLine();
        Assert.assertTrue(line.endsWith(msg));
        instances.add(Integer.parseInt(line.substring(0, line.length() - msg.length())));
      } finally {
        socket.close();
      }
    }
    return instances;
  }


  public static final class FailureRunnable extends SocketServer {

    private volatile boolean killed;

    @Override
    public void run() {
      killed = false;
      super.run();
      if (killed) {
        throw new RuntimeException("Exception");
      }
    }

    @Override
    public void handleCommand(Command command) throws Exception {
      if (command.getCommand().equals("kill" + getContext().getInstanceId())) {
        killed = true;
        running = false;
        serverSocket.close();
      }
    }

    @Override
    public void handleRequest(BufferedReader reader, PrintWriter writer) throws IOException {
      String line = reader.readLine();
      writer.println(getContext().getInstanceId() + line);
      writer.flush();
    }
  }
}
