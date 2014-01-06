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

import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Test for local file transfer.
 */
public class LocalFileTestRun {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testLocalFile() throws Exception {
    String header = Files.readFirstLine(new File(getClass().getClassLoader().getResource("header.txt").toURI()),
                                        Charsets.UTF_8);

    WeaveRunner runner = YarnTestUtils.getWeaveRunner();
    if (runner instanceof YarnWeaveRunnerService) {
      ((YarnWeaveRunnerService) runner).setJVMOptions("-verbose:gc -Xloggc:gc.log -XX:+PrintGCDetails");
    }

    WeaveController controller = runner.prepare(new LocalFileApplication())
      .withApplicationArguments("local")
      .withArguments("LocalFileSocketServer", "local2")
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    if (runner instanceof YarnWeaveRunnerService) {
      ((YarnWeaveRunnerService) runner).setJVMOptions("");
    }

    Iterable<Discoverable> discoverables = controller.discoverService("local");
    Assert.assertTrue(YarnTestUtils.waitForSize(discoverables, 1, 60));

    InetSocketAddress socketAddress = discoverables.iterator().next().getSocketAddress();
    Socket socket = new Socket(socketAddress.getAddress(), socketAddress.getPort());
    try {
      PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
      LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

      String msg = "Local file test";
      writer.println(msg);
      Assert.assertEquals(header, reader.readLine());
      Assert.assertEquals(msg, reader.readLine());
    } finally {
      socket.close();
    }

    controller.stopAndWait();

    Assert.assertTrue(YarnTestUtils.waitForSize(discoverables, 0, 60));

    TimeUnit.SECONDS.sleep(2);
  }

  public static final class LocalFileApplication implements WeaveApplication {

    private final File headerFile;

    public LocalFileApplication() throws Exception {
      // Create a jar file that contains the header.txt file inside.
      headerFile = tmpFolder.newFile("header.jar");
      JarOutputStream os = new JarOutputStream(new FileOutputStream(headerFile));
      try {
        os.putNextEntry(new JarEntry("header.txt"));
        ByteStreams.copy(getClass().getClassLoader().getResourceAsStream("header.txt"), os);
      } finally {
        os.close();
      }
    }

    @Override
    public WeaveSpecification configure() {
      return WeaveSpecification.Builder.with()
        .setName("LocalFileApp")
        .withRunnable()
          .add(new LocalFileSocketServer())
            .withLocalFiles()
              .add("header", headerFile, true).apply()
        .anyOrder()
        .build();
    }
  }

  public static final class LocalFileSocketServer extends SocketServer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSocketServer.class);

    @Override
    public void handleRequest(BufferedReader reader, PrintWriter writer) throws IOException {
      // Verify there is a gc.log file locally
      Preconditions.checkState(new File("gc.log").exists());

      LOG.info("handleRequest");
      String header = Files.toString(new File("header/header.txt"), Charsets.UTF_8);
      writer.write(header);
      writer.println(reader.readLine());
      LOG.info("Flushed response");
    }
  }
}
