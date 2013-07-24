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
package com.continuuity.weave.zookeeper;

import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import org.apache.zookeeper.CreateMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ZKOperationsTest {

  @Test
  public void recursiveDelete() throws ExecutionException, InterruptedException, TimeoutException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setTickTime(1000).build();
    zkServer.startAndWait();

    try {
      ZKClientService client = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      client.startAndWait();

      try {
        client.create("/test1/test10/test101", null, CreateMode.PERSISTENT).get();
        client.create("/test1/test10/test102", null, CreateMode.PERSISTENT).get();
        client.create("/test1/test10/test103", null, CreateMode.PERSISTENT).get();

        client.create("/test1/test11/test111", null, CreateMode.PERSISTENT).get();
        client.create("/test1/test11/test112", null, CreateMode.PERSISTENT).get();
        client.create("/test1/test11/test113", null, CreateMode.PERSISTENT).get();

        ZKOperations.recursiveDelete(client, "/test1").get(2, TimeUnit.SECONDS);

        Assert.assertNull(client.exists("/test1").get(2, TimeUnit.SECONDS));

      } finally {
        client.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }
}
