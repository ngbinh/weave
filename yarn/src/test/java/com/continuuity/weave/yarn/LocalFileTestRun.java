/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * Test for local file transfer.
 */
public class LocalFileTestRun {

  @Test
  public void testLocalFile() throws Exception {
    String header = Files.readFirstLine(new File(getClass().getClassLoader().getResource("header.txt").toURI()),
                                        Charsets.UTF_8);

    WeaveRunner runner = YarnTestSuite.getWeaveRunner();
    WeaveController controller = runner.prepare(new LocalFileApplication())
      .withApplicationArguments("local")
      .withArguments("LocalFileSocketServer", "local2")
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    Iterable<Discoverable> discoverables = controller.discoverService("local");
    Assert.assertTrue(YarnTestSuite.waitForSize(discoverables, 1, 60));

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

    Assert.assertTrue(YarnTestSuite.waitForSize(discoverables, 0, 60));

    TimeUnit.SECONDS.sleep(2);
  }

  public static final class LocalFileApplication implements WeaveApplication {

    private final File headerFile;

    public LocalFileApplication() throws URISyntaxException {
      headerFile = new File(getClass().getClassLoader().getResource("header.jar").toURI());
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
      LOG.info("handleRequest");
      String header = Files.toString(new File("header/header.txt"), Charsets.UTF_8);
      writer.write(header);
      writer.println(reader.readLine());
      LOG.info("Flushed response");
    }
  }
}
