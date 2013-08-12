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
package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.internal.RunIds;
import com.continuuity.weave.internal.WeaveContainerController;
import com.continuuity.weave.internal.state.Message;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A helper class for ApplicationMasterService to keep track of running containers and to interact
 * with them.
 */
final class RunningContainers {
  private static final Logger LOG = LoggerFactory.getLogger(RunningContainers.class);

  // Table of <runnableName, containerId, controller>
  private final Table<String, ContainerId, WeaveContainerController> containers;
  private final Deque<String> startSequence;
  private final Lock containerLock;
  private final Condition containerChange;

  RunningContainers() {
    containers = HashBasedTable.create();
    startSequence = Lists.newLinkedList();
    containerLock = new ReentrantLock();
    containerChange = containerLock.newCondition();
  }

  /**
   * Returns {@code true} if there is no live container.
   */
  boolean isEmpty() {
    containerLock.lock();
    try {
      return containers.columnMap().isEmpty();
    } finally {
      containerLock.unlock();
    }
  }

  void add(String runnableName, ContainerId container, WeaveContainerController controller) {
    containerLock.lock();
    try {
      containers.put(runnableName, container, controller);
      if (startSequence.isEmpty() || !runnableName.equals(startSequence.peekLast())) {
        startSequence.addLast(runnableName);
      }
      containerChange.signalAll();
    } finally {
      containerLock.unlock();
    }
  }

  RunId getBaseRunId(String runnableName) {
    containerLock.lock();
    try {
      Collection<WeaveContainerController> controllers = containers.row(runnableName).values();
      if (controllers.isEmpty()) {
        return RunIds.generate();
      }
      // A bit hacky, as it knows the naming convention of RunId as (base-[instanceId]).
      String id = controllers.iterator().next().getRunId().getId();
      return RunIds.fromString(id.substring(0, id.lastIndexOf('-')));
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Stops and removes the last running container of the given runnable.
   */
  void removeLast(String runnableName) {
    containerLock.lock();
    try {
      ContainerId containerId = null;
      WeaveContainerController lastController = null;
      int maxId = -1;
      for (Map.Entry<ContainerId, WeaveContainerController> entry : containers.row(runnableName).entrySet()) {
        // A bit hacky, as it knows the naming convention of RunId as (base-[instanceId]).
        // Could be more generic if using data structure other than a hashbased table.
        String id = entry.getValue().getRunId().getId();
        int instanceId = Integer.parseInt(id.substring(id.lastIndexOf('-') + 1));
        if (instanceId > maxId) {
          maxId = instanceId;
          containerId = entry.getKey();
          lastController = entry.getValue();
        }
      }
      if (lastController == null) {
        LOG.warn("No running container found for {}", runnableName);
        return;
      }
      LOG.info("Stopping service: {} {}", runnableName, lastController.getRunId());
      lastController.stopAndWait();
      containers.remove(runnableName, containerId);
      containerChange.signalAll();
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Blocks until there are changes in running containers.
   */
  void waitForCount(String runnableName, int count) throws InterruptedException {
    containerLock.lock();
    try {
      while (containers.row(runnableName).size() != count) {
        containerChange.await();
      }
    } finally {
      containerLock.unlock();
    }
  }

  int count(String runnableName) {
    containerLock.lock();
    try {
      return containers.row(runnableName).size();
    } finally {
      containerLock.unlock();
    }
  }

  void sendToAll(Message message, Runnable completion) {
    containerLock.lock();
    try {
      if (containers.isEmpty()) {
        completion.run();
      }

      // Sends the command to all running containers
      AtomicInteger count = new AtomicInteger(containers.size());
      for (Map.Entry<String, Map<ContainerId, WeaveContainerController>> entry : containers.rowMap().entrySet()) {
        for (WeaveContainerController controller : entry.getValue().values()) {
          sendMessage(entry.getKey(), message, controller, count, completion);
        }
      }
    } finally {
      containerLock.unlock();
    }
  }

  void sendToRunnable(String runnableName, Message message, Runnable completion) {
    containerLock.lock();
    try {
      Collection<WeaveContainerController> controllers = containers.row(runnableName).values();
      if (controllers.isEmpty()) {
        completion.run();
      }

      AtomicInteger count = new AtomicInteger(controllers.size());
      for (WeaveContainerController controller : controllers) {
        sendMessage(runnableName, message, controller, count, completion);
      }
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Stops all running services. Only called when the AppMaster stops.
   */
  void stopAll() {
    containerLock.lock();
    try {
      // Stop it one by one in reverse order of start sequence
      Iterator<String> itor = startSequence.descendingIterator();
      List<ListenableFuture<ServiceController.State>> futures = Lists.newLinkedList();
      while (itor.hasNext()) {
        String runnableName = itor.next();
        LOG.info("Stopping all instances of " + runnableName);

        futures.clear();
        // Parallel stops all running containers of the current runnable.
        for (WeaveContainerController controller : containers.row(runnableName).values()) {
          futures.add(controller.stop());
        }
        // Wait for containers to stop. Assumes the future returned by Futures.successfulAsList won't throw exception.
        Futures.getUnchecked(Futures.successfulAsList(futures));

        LOG.info("Terminated all instances of " + runnableName);
      }
    } finally {
      containerLock.unlock();
    }
  }

  Set<ContainerId> getContainerIds() {
    containerLock.lock();
    try {
      return ImmutableSet.copyOf(containers.columnKeySet());
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Handle completion of container.
   * @param status The completion status.
   * @param restartRunnables Set of runnable names that requires restart.
   */
  void handleCompleted(ContainerStatus status, Multiset<String> restartRunnables) {
    containerLock.lock();
    ContainerId containerId = status.getContainerId();
    int exitStatus = status.getExitStatus();
    ContainerState state = status.getState();

    try {
      Map<String, WeaveContainerController> lookup = containers.column(containerId);
      if (lookup.isEmpty()) {
        // It's OK because if a container is stopped through the controller this would be empty.
        return;
      }

      if (lookup.size() != 1) {
        LOG.warn("More than one controller found for container {}", containerId);
      }

      if (exitStatus != 0) {
        if (exitStatus == -100) {
          LOG.warn("Container {} exited abnormally with state {}, exit code {}.", containerId, state, exitStatus);
        } else {
          LOG.warn("Container {} exited unexpected with state{}, exit code{}. Re-request the container.",
                   containerId, state, exitStatus);
          restartRunnables.add(lookup.keySet().iterator().next());
        }
      }

      for (WeaveContainerController controller : lookup.values()) {
        controller.completed(exitStatus);
      }

      lookup.clear();
      containerChange.signalAll();
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Sends a command through the given {@link com.continuuity.weave.internal.WeaveContainerController} of a runnable. Decrements the count
   * when the sending of command completed. Triggers completion when count reaches zero.
   */
  private void sendMessage(final String runnableName, final Message message,
                           final WeaveContainerController controller, final AtomicInteger count,
                           final Runnable completion) {
    Futures.addCallback(controller.sendMessage(message), new FutureCallback<Message>() {
      @Override
      public void onSuccess(Message result) {
        if (count.decrementAndGet() == 0) {
          completion.run();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        try {
          LOG.error("Failed to send message. Runnable: {}, RunId: {}, Message: {}.",
                    runnableName, controller.getRunId(), message, t);
        } finally {
          if (count.decrementAndGet() == 0) {
            completion.run();
          }
        }
      }
    });
  }
}
