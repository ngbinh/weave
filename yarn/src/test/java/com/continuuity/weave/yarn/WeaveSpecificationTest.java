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
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.WeaveSpecification;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class WeaveSpecificationTest {

  /**
   * Dummy for test.
   */
  public static final class DummyRunnable extends AbstractWeaveRunnable {

    @Override
    public void stop() {
      // no-op
    }

    @Override
    public void run() {
      // no-op
    }
  }

  @Test
  public void testAnyOrder() {
    WeaveSpecification spec =
      WeaveSpecification.Builder.with()
        .setName("Testing")
        .withRunnable()
        .add("r1", new DummyRunnable()).noLocalFiles()
        .add("r2", new DummyRunnable()).noLocalFiles()
        .add("r3", new DummyRunnable()).noLocalFiles()
        .anyOrder()
        .build();

    Assert.assertEquals(3, spec.getRunnables().size());
    List<WeaveSpecification.Order> orders = spec.getOrders();
    Assert.assertEquals(1, orders.size());
    Assert.assertEquals(ImmutableSet.of("r1", "r2", "r3"), orders.get(0).getNames());
  }

  @Test
  public void testOrder() {
    WeaveSpecification spec =
      WeaveSpecification.Builder.with()
        .setName("Testing")
        .withRunnable()
        .add("r1", new DummyRunnable()).noLocalFiles()
        .add("r2", new DummyRunnable()).noLocalFiles()
        .add("r3", new DummyRunnable()).noLocalFiles()
        .add("r4", new DummyRunnable()).noLocalFiles()
        .withOrder().begin("r1", "r2").nextWhenStarted("r3")
        .build();

    Assert.assertEquals(4, spec.getRunnables().size());
    List<WeaveSpecification.Order> orders = spec.getOrders();
    Assert.assertEquals(3, orders.size());
    Assert.assertEquals(ImmutableSet.of("r1", "r2"), orders.get(0).getNames());
    Assert.assertEquals(ImmutableSet.of("r3"), orders.get(1).getNames());
    Assert.assertEquals(ImmutableSet.of("r4"), orders.get(2).getNames());
  }
}
