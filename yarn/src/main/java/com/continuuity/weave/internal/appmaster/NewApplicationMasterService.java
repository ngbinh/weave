/*
 * Copyright 2013 Continuuity,Inc.
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
package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.Arguments;
import com.continuuity.weave.internal.Constants;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.ZKServiceDecorator;
import com.continuuity.weave.internal.json.ArgumentsCodec;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.utils.Networks;
import com.continuuity.weave.internal.yarn.ports.AMRMClient;
import com.continuuity.weave.internal.yarn.ports.AMRMClientImpl;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Application master for WeaveApplication.
 */
public final class NewApplicationMasterService extends AbstractExecutionThreadService implements MessageCallback {

  private static final Logger LOG = LoggerFactory.getLogger(NewApplicationMasterService.class);

  private final Configuration yarnConf;
  private final RunId runId;
  private final ZKClient zkClient;
  private final WeaveSpecification weaveSpec;
  private final Arguments arguments;
  private final AMRMClient amrmClient;
  private final ZKServiceDecorator zkServiceDecorator;

  private YarnRPC yarnRPC;
  private EmbeddedKafkaServer kafkaServer;

  public NewApplicationMasterService(Configuration yarnConf, RunId runId, ZKClient zkClient) throws IOException {
    this.yarnConf = yarnConf;
    this.runId = runId;
    this.zkClient = zkClient;
    this.weaveSpec = WeaveSpecificationAdapter.create().fromJson(new File(Constants.Files.WEAVE_SPEC));
    this.arguments = ArgumentsCodec.decode(Files.newReaderSupplier(new File(Constants.Files.ARGUMENTS),
                                                                   Charsets.UTF_8));

    // Get the AM container ID from environment and creates AMRMClient
    String masterContainerId = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(masterContainerId != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    amrmClient = new AMRMClientImpl(ConverterUtils.toContainerId(masterContainerId).getApplicationAttemptId());

    // Creates the ZKServiceDecorator for reflecting state changes to Zookeeper and message handling
    ApplicationMasterLiveNodeData liveNodeData = new ApplicationMasterLiveNodeData(
      Integer.parseInt(System.getenv(EnvKeys.WEAVE_APP_ID)),
      Long.parseLong(System.getenv(EnvKeys.WEAVE_APP_ID_CLUSTER_TIME)),
      masterContainerId
    );
    zkServiceDecorator = new ZKServiceDecorator(zkClient, runId, createLiveNodeSupplier(liveNodeData), this);
  }

  private Supplier<? extends JsonElement> createLiveNodeSupplier(ApplicationMasterLiveNodeData liveNodeData) {
    final JsonElement json = new Gson().toJsonTree(liveNodeData);
    return new Supplier<JsonElement>() {
      @Override
      public JsonElement get() {
        return json;
      }
    };
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting application master with spec: " + WeaveSpecificationAdapter.create().toJson(weaveSpec));

    // Creates ZK path for runnable and kafka logging service
    Futures.allAsList(ImmutableList.of(
      zkClient.create("/" + runId.getId() + "/runnables", null, CreateMode.PERSISTENT),
      zkClient.create("/" + runId.getId() + "/kafka", null, CreateMode.PERSISTENT))
    ).get();

    // Starts kafka server
    LOG.info("Starting kafka server");
    kafkaServer = new EmbeddedKafkaServer(new File(Constants.Files.KAFKA), generateKafkaConfig());
    kafkaServer.startAndWait();
    LOG.info("Kafka server started");


    // Creates the YARN RPC and AMRMClient for running AM-RM protocol
    yarnRPC = YarnRPC.create(yarnConf);
    amrmClient.init(yarnConf);
    amrmClient.start();

    // Register this AM with the RM.
    amrmClient.registerApplicationMaster("", 0, null);

    LOG.info("Application master started.");
  }

  @Override
  protected void shutDown() throws Exception {

  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {

    }
  }

  @Override
  public ListenableFuture<String> onReceived(String messageId, Message message) {
    return Futures.immediateFuture(messageId);
  }

  private Properties generateKafkaConfig() {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("log.dir", new File("kafka-logs").getAbsolutePath());
    prop.setProperty("zk.connect", getKafkaZKConnect());
    prop.setProperty("num.threads", "8");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("zk.connectiontimeout.ms", "1000000");
    prop.setProperty("socket.receive.buffer", "1048576");
    prop.setProperty("enable.zookeeper", "true");
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("brokerid", "0");
    prop.setProperty("socket.send.buffer", "1048576");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.file.size", "536870912");
    prop.setProperty("log.default.flush.interval.ms", "1000");
    return prop;
  }

  private String getKafkaZKConnect() {
    return String.format("%s/%s/kafka", zkClient.getConnectString(), runId.getId());
  }
}
