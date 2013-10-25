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

package com.continuuity.weave.discovery;

import com.continuuity.weave.common.Cancellable;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Test memory based service discovery service.
 */
public class InMemoryDiscoveryServiceTest {
  private Cancellable register(DiscoveryService service, final String name, final String host, final int port) {
    return service.register(new Discoverable() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(host, port);
      }
    });
  }

  @Test
  public void simpleDiscoverable() throws Exception {
    DiscoveryService discoveryService = new InMemoryDiscoveryService();
    DiscoveryServiceClient discoveryServiceClient = (DiscoveryServiceClient) discoveryService;

    // Register one service running on one host:port
    Cancellable cancellable = register(discoveryService, "foo", "localhost", 8090);
    Iterable<Discoverable> discoverables = discoveryServiceClient.discover("foo");

    // Discover that registered host:port.
    Assert.assertTrue(Iterables.size(discoverables) == 1);

    // Remove the service
    cancellable.cancel();

    // There should be no service.
    discoverables = discoveryServiceClient.discover("foo");
    TimeUnit.MILLISECONDS.sleep(100);
    Assert.assertTrue(Iterables.size(discoverables) == 0);
  }
}
