package com.continuuity.weave.yarn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Test server that returns back the value of the env key sent in.  Used to check env for
 * runnables is correctly set.
 */
public class EnvironmentEchoServer extends SocketServer {

  @Override
  public void handleRequest(BufferedReader reader, PrintWriter writer) throws IOException {
    String envKey = reader.readLine();
    writer.println(System.getenv(envKey));
  }
}
