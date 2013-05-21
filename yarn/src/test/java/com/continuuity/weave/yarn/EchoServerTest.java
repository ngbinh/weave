package com.continuuity.weave.yarn;

import com.continuuity.weave.api.ListenerAdapter;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class EchoServerTest {

  @Test
  public void testEchoServer() throws InterruptedException, ExecutionException, IOException {
    WeaveController controller = runnerService.prepare(new EchoServer(),
                                                       ResourceSpecification.Builder.with()
                                                         .setCores(1)
                                                         .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
                                                         .setInstances(2)
                                                         .build())
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
    int trial = 0;
    while (Iterables.size(echoServices) != 2 && trial < 60) {
      TimeUnit.SECONDS.sleep(1);
      trial++;
    }
    Assert.assertTrue(trial < 60);

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

    controller = runnerService.lookup("EchoServer", controller.getRunId());
    controller.addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));

    controller.stop().get();

    TimeUnit.SECONDS.sleep(2);
  }

  @Before
  public void init() throws IOException {
    // Starts Zookeeper
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    // Start YARN mini cluster
    YarnConfiguration config = new YarnConfiguration(new Configuration());

    // TODO: Hack
    config.set("yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler" +
      ".fifo.FifoScheduler");
    config.set("yarn.minicluster.fixed.ports", "true");
    config.set("yarn.application.classpath",
               Joiner.on(',').join(
                 Splitter.on(System.getProperty("path.separator")).split(System.getProperty("java.class.path"))));

    cluster = new MiniYARNCluster("test-cluster", 1, 1, 1);
    cluster.init(config);
    cluster.start();

    runnerService = new YarnWeaveRunnerService(config, zkServer.getConnectionStr() + "/weave",
                                               new LocalLocationFactory(Files.createTempDir()));
    runnerService.startAndWait();
  }

  @After
  public void finish() {
    runnerService.stopAndWait();
//    cluster.stop();
//    zkServer.stopAndWait();
  }

  private InMemoryZKServer zkServer;
  private MiniYARNCluster cluster;
  private WeaveRunnerService runnerService;
}
