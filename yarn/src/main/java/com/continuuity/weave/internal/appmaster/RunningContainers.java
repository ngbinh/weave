/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
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
import com.google.common.collect.Table;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

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
*
*/
final class RunningContainers {
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

  void add(String runnableName, Container container, WeaveContainerController controller) {
    containerLock.lock();
    try {
      containers.put(runnableName, container.getId(), controller);
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
   * Stops and remove the last running container of the given runnable.
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
        ApplicationMasterService.LOG.warn("No running container found for {}", runnableName);
        return;
      }
      ApplicationMasterService.LOG.info("Stopping service: {} {}", runnableName, lastController.getRunId());
      lastController.stopAndWait();
      containers.remove(runnableName, containerId);
      containerChange.signalAll();
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Blocks until there is changes in running containers.
   */
  void waitForChange() throws InterruptedException {
    containerLock.lock();
    try {
      containerChange.await();
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
   * Stops all running services. Only called when the AppMaster stop.
   */
  void stopAll() {
    containerLock.lock();
    try {
      // Stop it one by one in reverse order of start sequence
      Iterator<String> itor = startSequence.descendingIterator();
      List<ListenableFuture<ServiceController.State>> futures = Lists.newLinkedList();
      while (itor.hasNext()) {
        String runnableName = itor.next();
        ApplicationMasterService.LOG.info("Stopping all instances of " + runnableName);

        futures.clear();
        // Parallel stops all running containers of the current runnable.
        for (WeaveContainerController controller : containers.row(runnableName).values()) {
          futures.add(controller.stop());
        }
        // Wait for containers to stop. Assuming the future returned by Futures.successfulAsList won't throw exception.
        Futures.getUnchecked(Futures.successfulAsList(futures));

        ApplicationMasterService.LOG.info("Terminated all instances of " + runnableName);
      }
      containerChange.signalAll();
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
          ApplicationMasterService.LOG.error("Failed to send message. Runnable: {}, RunId: {}, Message: {}.",
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
