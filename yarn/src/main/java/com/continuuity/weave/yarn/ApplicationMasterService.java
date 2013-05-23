/**
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
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

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.ProcessLauncher;
import com.continuuity.weave.internal.RunIds;
import com.continuuity.weave.internal.WeaveContainerLauncher;
import com.continuuity.weave.internal.json.LocalFileCodec;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.state.ZKServiceDecorator;
import com.continuuity.weave.internal.yarn.ports.AMRMClient;
import com.continuuity.weave.internal.yarn.ports.AMRMClientImpl;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class ApplicationMasterService implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterService.class);

  private final RunId runId;
  private final String zkConnectStr;
  private final ZKClientService zkClientService;
  private final WeaveSpecification weaveSpec;
  private final ListMultimap<String, String> runnableArgs;
  private final YarnConfiguration yarnConf;
  private final String masterContainerId;
  private final AMRMClient amrmClient;
  private final ZKServiceDecorator serviceDelegate;
  private final RunningContainers runningContainers;

  // TODO (terence) : This is temporary to fix issue when shutting down. Need to unify it with WeaveContainerMain
  private final AsyncFunction<State, State> stopZKClient = new AsyncFunction<State, State>() {
    @Override
    public ListenableFuture<State> apply(State input) throws Exception {
      return zkClientService.stop();
    }
  };

  private YarnRPC yarnRPC;
  private Resource maxCapability;
  private Resource minCapability;


  public ApplicationMasterService(RunId runId, String zkConnectStr, File weaveSpecFile) throws IOException {
    this.runId = runId;
    this.zkConnectStr = zkConnectStr;
    this.weaveSpec = WeaveSpecificationAdapter.create().fromJson(weaveSpecFile);
    this.runnableArgs = decodeRunnableArgs();

    this.yarnConf = new YarnConfiguration();

    this.zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(zkConnectStr).build(),
                                 RetryStrategies.fixDelay(1, TimeUnit.SECONDS))));
    this.serviceDelegate = new ZKServiceDecorator(zkClientService, runId,
                                                  createLiveNodeDataSupplier(), new ServiceDelegate());

    // Get the container ID and convert it to ApplicationAttemptId
    masterContainerId = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(masterContainerId != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    amrmClient = new AMRMClientImpl(ConverterUtils.toContainerId(masterContainerId).getApplicationAttemptId());

    runningContainers = new RunningContainers();
  }

  private ListMultimap<String, String> decodeRunnableArgs() throws IOException {
    ListMultimap<String, String> result = ArrayListMultimap.create();

    Map<String, Collection<String>> decoded = new Gson().fromJson(System.getenv(EnvKeys.WEAVE_RUNNABLE_ARGS),
                                                                  new TypeToken<Map<String, Collection<String>>>() {
                                                                  }.getType());
    for (Map.Entry<String, Collection<String>> entry : decoded.entrySet()) {
      result.putAll(entry.getKey(), entry.getValue());
    }
    return result;
  }

  private Supplier<? extends JsonElement> createLiveNodeDataSupplier() {
    return new Supplier<JsonElement>() {
      @Override
      public JsonElement get() {
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty("appId", Integer.parseInt(System.getenv(EnvKeys.WEAVE_APP_ID)));
        jsonObj.addProperty("appIdClusterTime", Long.parseLong(System.getenv(EnvKeys.WEAVE_APP_ID_CLUSTER_TIME)));
        jsonObj.addProperty("containerId", masterContainerId);
        return jsonObj;
      }
    };
  }

  private void doStart() throws Exception {
    LOG.info("Start application master with spec: " + WeaveSpecificationAdapter.create().toJson(weaveSpec));

    yarnRPC = YarnRPC.create(yarnConf);

    amrmClient.init(yarnConf);
    amrmClient.start();
    // TODO: Have RPC host and port
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster("", 0, null);
    maxCapability = response.getMaximumResourceCapability();
    minCapability = response.getMinimumResourceCapability();

    LOG.info("Maximum resource capability: " + maxCapability);
    LOG.info("Minimum resource capability: " + minCapability);

    // Creates ZK path for runnable and kafka logging service
    Futures.allAsList(ImmutableList.of(
      zkClientService.create("/" + runId.getId() + "/runnables", null, CreateMode.PERSISTENT),
      zkClientService.create("/" + runId.getId() + "/kafka", null, CreateMode.PERSISTENT))).get();
  }

  private void doStop() throws Exception {
    Thread.interrupted();     // This is just to clear the interrupt flag

    LOG.info("Stop application master with spec: " + WeaveSpecificationAdapter.create().toJson(weaveSpec));

    Set<ContainerId> ids = Sets.newHashSet(runningContainers.getContainerIds());

    runningContainers.stopAll();

    int count = 0;
    while (!ids.isEmpty() && count++ < 5) {
      AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
      for (ContainerStatus status : allocateResponse.getAMResponse().getCompletedContainersStatuses()) {
        ids.remove(status.getContainerId());
      }
      TimeUnit.SECONDS.sleep(1);
    }

    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
    amrmClient.stop();
  }

  private void doRun() throws Exception {

    // Orderly stores container requests.
    Queue<RunnableContainerRequest> runnableRequests = Lists.newLinkedList();
    for (WeaveSpecification.Order order : weaveSpec.getOrders()) {

      ImmutableMultimap.Builder<Resource, RuntimeSpecification> builder = ImmutableMultimap.builder();
      for (String runnableName : order.getNames()) {
        RuntimeSpecification runtimeSpec = weaveSpec.getRunnables().get(runnableName);
        Resource capability = createCapability(runtimeSpec.getResourceSpecification());
        builder.put(capability, runtimeSpec);
      }
      runnableRequests.add(new RunnableContainerRequest(order.getType(), builder.build()));
    }

    // The main loop
    Map.Entry<Resource, ? extends Collection<RuntimeSpecification>> currentRequest = null;
    Queue<ProvisionRequest> provisioning = Lists.newLinkedList();
    while (isRunning()) {
      // If nothing is in provisioning, and no pending request, move to next one
      while (provisioning.isEmpty() && currentRequest == null && !runnableRequests.isEmpty()) {
        currentRequest = runnableRequests.peek().takeRequest();
        if (currentRequest == null) {
          // All different types of resource request from current order is done, move to next one
          // TODO: Need to handle order type as well
          runnableRequests.poll();
        }
      }
      // Nothing in provision, makes the next batch of provision request
      if (provisioning.isEmpty() && currentRequest != null) {
        provisioning.addAll(addContainerRequests(currentRequest.getKey(), currentRequest.getValue()));
        currentRequest = null;
      }

      // Now call allocate
      AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
      allocateResponse.getAMResponse().getResponseId();
      AMResponse amResponse = allocateResponse.getAMResponse();

      // Assign runnable to container
      launchRunnable(amResponse.getAllocatedContainers(), provisioning);
      TimeUnit.SECONDS.sleep(1);
    }
  }

  /**
   * Adds {@link AMRMClient.ContainerRequest}s with the given resource capability for each runtime.
   * @param capability
   * @param runtimeSpecs
   */
  private Queue<ProvisionRequest> addContainerRequests(Resource capability,
                                                       Collection<RuntimeSpecification> runtimeSpecs) {
    Queue<ProvisionRequest> requests = Lists.newLinkedList();
    for (RuntimeSpecification runtimeSpec : runtimeSpecs) {
      // TODO: Allow user to set priority?
      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      int containerCount = runtimeSpec.getResourceSpecification().getInstances();
      AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null,
                                                                            priority, containerCount);
      LOG.info("Request for container: " + request);
      requests.add(new ProvisionRequest(request, runtimeSpec));
      amrmClient.addContainerRequest(request);
    }
    return requests;
  }

  /**
   * Launches runnables in the provisioned containers.
   * @param containers
   * @param provisioning
   */
  private void launchRunnable(List<Container> containers, Queue<ProvisionRequest> provisioning) {
    for (Container container : containers) {
      ProvisionRequest provisionRequest = provisioning.peek();
      if (provisionRequest == null) {
        LOG.info("Nothing to run in container, releasing it: " + container);
        amrmClient.releaseAssignedContainer(container.getId());
        continue;
      }

      String runnableName = provisionRequest.getRuntimeSpec().getName();
      int containerCount = provisionRequest.getRuntimeSpec().getResourceSpecification().getInstances();
      int instanceId = runningContainers.count(runnableName);

      RunId containerRunId = RunIds.fromString(provisionRequest.getBaseRunId().getId() + "-" + instanceId);
      ProcessLauncher processLauncher = new DefaultProcessLauncher(
        container, yarnRPC, yarnConf, getKafkaZKConnect(),
        getLocalFiles(),
        ImmutableMap.<String, String>builder()
         .put(EnvKeys.WEAVE_APP_RUN_ID, runId.getId())
         .put(EnvKeys.WEAVE_ZK_CONNECT, zkConnectStr)
         .put(EnvKeys.WEAVE_APPLICATION_ARGS, System.getenv(EnvKeys.WEAVE_APPLICATION_ARGS))
         .build()
        );

      LOG.info("Starting runnable " + runnableName + " in container " + container);

      WeaveContainerLauncher launcher = new WeaveContainerLauncher(weaveSpec.getRunnables().get(runnableName),
                                                                   containerRunId, processLauncher,
                                                                   ZKClients.namespace(zkClientService,
                                                                                       getZKNamespace(runnableName)),
                                                                   runnableArgs.get(runnableName),
                                                                   instanceId);
      runningContainers.add(runnableName, container,
                            launcher.start(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
                                           ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

      if (runningContainers.count(runnableName) == containerCount) {
        LOG.info("Runnable " + runnableName + " fully provisioned with " + containerCount + " instances.");
        provisioning.poll();
      }
    }
  }

  private List<LocalFile> getLocalFiles() {
    try {
      Reader reader = Files.newReader(new File("localFiles.json"), Charsets.UTF_8);
      try {
        return new GsonBuilder().registerTypeAdapter(LocalFile.class, new LocalFileCodec())
                                .create().fromJson(reader, new TypeToken<List<LocalFile>>() {}.getType());
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a {@link File} from the path given in the environment.
   */
  private File createFile(String envKey) {
    return new File(System.getenv(envKey));
  }

  private String getZKNamespace(String runnableName) {
    return String.format("/%s/runnables/%s", runId.getId(), runnableName);
  }

  private String getKafkaZKConnect() {
    return String.format("%s/%s/kafka", zkConnectStr, runId.getId());
  }

  private ListenableFuture<String> processMessage(String messageId, Message message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Message received: " + messageId + " " + message);
    }

    SettableFuture<String> result = SettableFuture.create();
    if (handleSetInstances(message, getMessageCompletion(messageId, result))) {
      return result;
    }

    // TODO
    if (message.getScope() == Message.Scope.ALL_RUNNABLE) {
      for (String runnableName : weaveSpec.getRunnables().keySet()) {
        ZKClient zkClient = ZKClients.namespace(zkClientService, getZKNamespace(runnableName));
//        ZKMessages.sendMessage(zkCl)
      }
    }

    return result;
  }

  private boolean handleSetInstances(Message message, Runnable completion) {
    // TODO
    return true;
  }

  private Runnable getMessageCompletion(final String messageId, final SettableFuture<String> future) {
    return new Runnable() {
      @Override
      public void run() {
        future.set(messageId);
      }
    };
  }

  private Resource createCapability(ResourceSpecification resourceSpec) {
    Resource capability = Records.newRecord(Resource.class);

    int cores = resourceSpec.getCores();
    // The following hack is to workaround the still unstable Resource interface.
    // With the latest YARN API, it should be like this:
//    int cores = Math.max(Math.min(resourceSpec.getCores(), maxCapability.getVirtualCores()),
//                         minCapability.getVirtualCores());
//    capability.setVirtualCores(cores);
    try {
      Method getVirtualCores = Resource.class.getMethod("getVirtualCores");
      Method setVirtualCores = Resource.class.getMethod("setVirtualCores", int.class);

      cores = Math.max(Math.min(cores, (Integer) getVirtualCores.invoke(maxCapability)),
                       (Integer) getVirtualCores.invoke(minCapability));
      setVirtualCores.invoke(capability, cores);
    } catch (Exception e) {
      // It's ok to ignore this exception, as it's using older version of API.
      LOG.info("Virtual cores limit not supported.");
    }

    int memory = Math.max(Math.min(resourceSpec.getMemorySize(), maxCapability.getMemory()),
                          minCapability.getMemory());
    capability.setMemory(memory);

    return capability;
  }

  @Override
  public ListenableFuture<State> start() {
    return Futures.transform(zkClientService.start(), new AsyncFunction<State, State>() {
      @Override
      public ListenableFuture<State> apply(State input) throws Exception {
        return serviceDelegate.start();
      }
    });
  }

  @Override
  public State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public boolean isRunning() {
    return serviceDelegate.isRunning();
  }

  @Override
  public State state() {
    return serviceDelegate.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return Futures.transform(serviceDelegate.stop(), stopZKClient);
  }

  @Override
  public State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    serviceDelegate.addListener(listener, executor);
  }

  /**
   * A private class for service lifecycle. It's done this way so that we can have {@link ZKServiceDecorator} to
   * wrap around this to reflect status in ZK.
   */
  private final class ServiceDelegate extends AbstractExecutionThreadService implements MessageCallback {

    private volatile Thread runThread;

    @Override
    protected void run() throws Exception {
      runThread = Thread.currentThread();
      try {
        doRun();
      } catch (InterruptedException e) {
        // It's ok to get interrupted exception, as it's a signal to stop
        Thread.currentThread().interrupt();
      }
    }

    @Override
    protected void startUp() throws Exception {
      doStart();
    }

    @Override
    protected void shutDown() throws Exception {
      doStop();
    }

    @Override
    protected void triggerShutdown() {
      Thread runThread = this.runThread;
      if (runThread != null) {
        runThread.interrupt();
      }
    }

    @Override
    public ListenableFuture<String> onReceived(String messageId, Message message) {
      return processMessage(messageId, message);
    }
  }

  private static final class RunnableContainerRequest {
    private final WeaveSpecification.Order.Type orderType;
    private final Iterator<Map.Entry<Resource, Collection<RuntimeSpecification>>> requests;

    private RunnableContainerRequest(WeaveSpecification.Order.Type orderType,
                                     Multimap<Resource, RuntimeSpecification> requests) {
      this.orderType = orderType;
      this.requests = requests.asMap().entrySet().iterator();
    }

    private WeaveSpecification.Order.Type getOrderType() {
      return orderType;
    }

    /**
     * Remove a resource request and returns it.
     * @return The {@link Resource} and {@link Collection} of {@link RuntimeSpecification} or
     *         {@code null} if there is no more request.
     */
    private Map.Entry<Resource, ? extends Collection<RuntimeSpecification>> takeRequest() {
      Map.Entry<Resource, Collection<RuntimeSpecification>> next = Iterators.getNext(requests, null);
      return next == null ? null : Maps.immutableEntry(next.getKey(), ImmutableList.copyOf(next.getValue()));
    }
  }

  private static final class RunningContainers {
    private final Table<String, ContainerId, ServiceController> runningContainers;
    private final Deque<String> startSequence;

    RunningContainers() {
      runningContainers = HashBasedTable.create();
      startSequence = Lists.newLinkedList();
    }

    synchronized void add(String runnableName, Container container, ServiceController controller) {
      runningContainers.put(runnableName, container.getId(), controller);
      if (startSequence.isEmpty() || !runnableName.equals(startSequence.peekLast())) {
        startSequence.addLast(runnableName);
      }
    }

    synchronized int count(String runnableName) {
      return runningContainers.row(runnableName).size();
    }

    synchronized void stopAll() {
      // Stop it one by one in reverse order of start sequence
      Iterator<String> itor = startSequence.descendingIterator();
      List<ListenableFuture<ServiceController.State>> futures = Lists.newLinkedList();
      while (itor.hasNext()) {
        String runnableName = itor.next();
        LOG.info("Stopping all instances of " + runnableName);

        futures.clear();
        // Parallel stops all running containers of the current runnable.
        for (ServiceController controller : runningContainers.row(runnableName).values()) {
          futures.add(controller.stop());
        }
        // Wait for containers to stop. Assuming the future returned by Futures.successfulAsList won't throw exception.
        Futures.getUnchecked(Futures.successfulAsList(futures));

        LOG.info("Terminated all instances of " + runnableName);
      }
    }

    synchronized Set<ContainerId> getContainerIds() {
      return ImmutableSet.copyOf(runningContainers.columnKeySet());
    }
  }
}
