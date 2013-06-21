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
package com.continuuity.weave.internal.zookeeper;

import com.continuuity.weave.common.Threads;
import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.OperationFuture;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 * A {@link ZKClient} that namespace every paths.
 */
public final class NamespaceZKClient implements ZKClient {

  private final String namespace;
  private final ZKClient delegate;
  private final String connectString;

  public NamespaceZKClient(ZKClient delegate, String namespace) {
    this.namespace = namespace;
    this.delegate = delegate;
    this.connectString = delegate.getConnectString() + namespace;
  }

  @Override
  public String getConnectString() {
    return connectString;
  }

  @Override
  public void addConnectionWatcher(Watcher watcher) {
    delegate.addConnectionWatcher(watcher);
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode) {
    return relayPath(delegate.create(namespace + path, data, createMode), this.<String>createFuture(path));
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode,
                                        boolean createParent) {
    return relayPath(delegate.create(namespace + path, data, createMode, createParent),
                     this.<String>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> exists(String path) {
    return relayFuture(delegate.exists(namespace + path), this.<Stat>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> exists(String path, @Nullable Watcher watcher) {
    return relayFuture(delegate.exists(namespace + path, watcher), this.<Stat>createFuture(path));
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path) {
    return relayFuture(delegate.getChildren(namespace + path), this.<NodeChildren>createFuture(path));
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher) {
    return relayFuture(delegate.getChildren(namespace + path, watcher), this.<NodeChildren>createFuture(path));
  }

  @Override
  public OperationFuture<NodeData> getData(String path) {
    return relayFuture(delegate.getData(namespace + path), this.<NodeData>createFuture(path));
  }

  @Override
  public OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher) {
    return relayFuture(delegate.getData(namespace + path, watcher), this.<NodeData>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> setData(String path, byte[] data) {
    return relayFuture(delegate.setData(namespace + path, data), this.<Stat>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
    return relayFuture(delegate.setData(namespace + dataPath, data, version), this.<Stat>createFuture(dataPath));
  }

  @Override
  public OperationFuture<String> delete(String path) {
    return relayPath(delegate.delete(namespace + path), this.<String>createFuture(path));
  }

  @Override
  public OperationFuture<String> delete(String deletePath, int version) {
    return relayPath(delegate.delete(namespace + deletePath, version), this.<String>createFuture(deletePath));
  }

  private <V> SettableOperationFuture<V> createFuture(String path) {
    return SettableOperationFuture.create(namespace + path, Threads.SAME_THREAD_EXECUTOR);
  }

  private <V> OperationFuture<V> relayFuture(final OperationFuture<V> from, final SettableOperationFuture<V> to) {
    Futures.addCallback(from, new FutureCallback<V>() {
      @Override
      public void onSuccess(V result) {
        to.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        to.setException(t);
      }
    });
    return to;
  }

  private OperationFuture<String> relayPath(final OperationFuture<String> from,
                                            final SettableOperationFuture<String> to) {
    from.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          String path = from.get();
          to.set(path.substring(namespace.length()));
        } catch (Exception e) {
          to.setException(e.getCause());
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return to;
  }
}
