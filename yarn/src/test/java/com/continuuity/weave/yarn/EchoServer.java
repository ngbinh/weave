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
public final class EchoServer extends AbstractWeaveRunnable {

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
        context.announce(context.getArguments()[0], serverSocket.getLocalPort())
      );
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
            LOG.info("Received: " + line);
            if (line != null) {
              writer.println(line);
            }
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
    LOG.info("Command received: " + command + " " + getContext().getInstanceCount());
  }
}
