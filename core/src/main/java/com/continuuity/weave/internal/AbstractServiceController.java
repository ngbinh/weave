/*
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
package com.continuuity.weave.internal;

import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.internal.json.StackTraceElementCodec;
import com.continuuity.weave.internal.json.StateNodeCodec;
import com.continuuity.weave.internal.state.Messages;
import com.continuuity.weave.internal.state.StateNode;
import com.continuuity.weave.internal.state.SystemMessages;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKOperations;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract base class for controlling remote service through ZooKeeper.
 */
public abstract class AbstractServiceController implements ServiceController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceController.class);

  private final RunId runId;
  private final AtomicReference<State> state;
  private final ListenerExecutors listenerExecutors;
  private final ZKClient zkClient;
  private final AtomicReference<byte[]> liveNodeData;
  private Cancellable watchCanceller;

  protected AbstractServiceController(ZKClient zkClient, RunId runId) {
    this.zkClient = zkClient;
    this.runId = runId;
    this.state = new AtomicReference<State>();
    this.listenerExecutors = new ListenerExecutors();
    this.liveNodeData = new AtomicReference<byte[]>();
  }

  public void start() {
    // Watch for state changes
    final Cancellable cancelState = ZKOperations.watchData(zkClient, getZKPath("state"),
                                                           new ZKOperations.DataCallback() {
      @Override
      public void updated(NodeData nodeData) {
        StateNode stateNode = decode(nodeData);
        if (state.getAndSet(stateNode.getState()) != stateNode.getState()) {
          fireStateChange(stateNode);
        }
      }
    });
    // Watch for instance node data
    final Cancellable cancelInstance = ZKOperations.watchData(zkClient, "/instances/" + runId.getId(),
                                                              new ZKOperations.DataCallback() {
      @Override
      public void updated(NodeData nodeData) {
        liveNodeData.set(nodeData.getData());
      }
    });
    watchCanceller = new Cancellable() {
      @Override
      public void cancel() {
        cancelState.cancel();
        cancelInstance.cancel();
      }
    };
  }

  protected byte[] getLiveNodeData() {
    return liveNodeData.get();
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public ListenableFuture<Command> sendCommand(Command command) {
    return ZKMessages.sendMessage(zkClient, getZKPath("messages/msg"), Messages.createForAll(command), command);
  }

  @Override
  public ListenableFuture<Command> sendCommand(String runnableName, Command command) {
    return ZKMessages.sendMessage(zkClient, getZKPath("messages/msg"),
                                  Messages.createForRunnable(runnableName, command), command);
  }

  @Override
  public State getState() {
    return state.get();
  }

  @Override
  public ListenableFuture<State> stop() {
    State oldState = state.getAndSet(State.STOPPING);
    if (oldState == null || oldState == State.STARTING || oldState == State.RUNNING) {
      watchCanceller.cancel();
      return Futures.transform(
        ZKMessages.sendMessage(zkClient, getZKPath("messages/msg"), SystemMessages.stopApplication(),
                               State.TERMINATED), new AsyncFunction<State, State>() {
        @Override
        public ListenableFuture<State> apply(State input) throws Exception {
          // Wait for the instance ephemeral node goes away
          return Futures.transform(
            ZKOperations.watchDeleted(zkClient, "/instances/" + runId.getId()), new Function<String, State>() {
               @Override
               public State apply(String input) {
                 LOG.info("Remote service stopped: " + runId.getId());
                 state.set(State.TERMINATED);
                 fireStateChange(new StateNode(State.TERMINATED, null));
                 return State.TERMINATED;
               }
             });
        }
      });
    }
    state.set(oldState);
    return Futures.immediateFuture(getState());
  }

  @Override
  public void stopAndWait() {
    Futures.getUnchecked(stop());
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    listenerExecutors.addListener(listener, executor);
  }

  private StateNode decode(NodeData nodeData) {
    // Node data and data inside shouldn't be null. If it does, the service must not be running anymore.
    if (nodeData == null) {
      return new StateNode(State.TERMINATED, null);
    }
    byte[] data = nodeData.getData();
    if (data == null) {
      return new StateNode(State.TERMINATED, null);
    }
    return new GsonBuilder().registerTypeAdapter(StateNode.class, new StateNodeCodec())
      .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
      .create()
      .fromJson(new String(data, Charsets.UTF_8), StateNode.class);
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", runId.getId(), path);
  }

  private void fireStateChange(StateNode state) {
    switch (state.getState()) {
      case STARTING:
        listenerExecutors.starting();
        break;
      case RUNNING:
        listenerExecutors.running();
        break;
      case STOPPING:
        listenerExecutors.stopping();
        break;
      case TERMINATED:
        listenerExecutors.terminated();
        break;
      case FAILED:
        listenerExecutors.failed(state.getStackTraces());
        break;
    }
  }

  private static final class ListenerExecutors implements Listener {
    private final Queue<ListenerExecutor> listeners = new ConcurrentLinkedQueue<ListenerExecutor>();
    private final ConcurrentMap<State, Boolean> callStates = Maps.newConcurrentMap();

    void addListener(Listener listener, Executor executor) {
      listeners.add(new ListenerExecutor(listener, executor));
    }

    @Override
    public void starting() {
      if (hasCalled(State.STARTING)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.starting();
      }
    }

    @Override
    public void running() {
      if (hasCalled(State.RUNNING)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.running();
      }
    }

    @Override
    public void stopping() {
      if (hasCalled(State.STOPPING)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.stopping();
      }
    }

    @Override
    public void terminated() {
      if (hasCalled(State.TERMINATED) || hasCalled(State.FAILED)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.terminated();
      }
    }

    @Override
    public void failed(StackTraceElement[] stackTraces) {
      if (hasCalled(State.FAILED) || hasCalled(State.TERMINATED)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.failed(stackTraces);
      }
    }

    private boolean hasCalled(State state) {
      return callStates.putIfAbsent(state, true) != null;
    }
  }

  private static final class ListenerExecutor implements Listener {

    private final Listener delegate;
    private final Executor executor;

    private ListenerExecutor(Listener delegate, Executor executor) {
      this.delegate = delegate;
      this.executor = executor;
    }

    @Override
    public void starting() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.starting();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void running() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.running();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void stopping() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.stopping();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void terminated() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.terminated();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void failed(final StackTraceElement[] stackTraces) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.failed(stackTraces);
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }
  }
}
