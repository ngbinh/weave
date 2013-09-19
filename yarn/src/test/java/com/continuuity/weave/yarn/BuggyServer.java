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

import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.common.Cancellable;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;

/**
 *
 */
public final class BuggyServer extends AbstractWeaveRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(EchoServer.class);

  private volatile boolean running;
  private volatile Thread runThread;
  private ServerSocket serverSocket;
  private Cancellable canceller;

  @Override
  public void initialize(WeaveContext context) {
    super.initialize(context);
    running = true;
    try {
      serverSocket = new ServerSocket(0);
      LOG.info("EchoServer started: " + serverSocket.getLocalSocketAddress() +
                 ", id: " + context.getInstanceId() +
                 ", count: " + context.getInstanceCount());

      final List<Cancellable> cancellables = ImmutableList.of(
        context.announce(context.getApplicationArguments()[0], serverSocket.getLocalPort()),
        context.announce(context.getArguments()[0], serverSocket.getLocalPort()));
      canceller = new Cancellable() {
        @Override
        public void cancel() {
          for (Cancellable c : cancellables) {
            c.cancel();
          }
        }
      };
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void run() {
    try {
      runThread = Thread.currentThread();
      while (running) {
        try {
          Socket socket = serverSocket.accept();
          try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            String line = reader.readLine();
            LOG.info("Received: " + line + " going to divide by it");
            Integer toDivide = Integer.valueOf(line);
            writer.println(Integer.toString(100 / toDivide));
          } finally {
            socket.close();
          }
        } catch (SocketException e) {
          LOG.info("Socket exception: " + e);
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping echo server");
    canceller.cancel();
    running = false;
    Thread t = runThread;
    if (t != null) {
      t.interrupt();
    }
    try {
      serverSocket.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
  }
}
