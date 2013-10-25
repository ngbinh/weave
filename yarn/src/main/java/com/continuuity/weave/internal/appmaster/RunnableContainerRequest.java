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

import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Data structure for holding set of runnable specifications based on resource capability.
 */
final class RunnableContainerRequest {
  private final WeaveSpecification.Order.Type orderType;
  private final Iterator<Map.Entry<Resource, Collection<RuntimeSpecification>>> requests;

  RunnableContainerRequest(WeaveSpecification.Order.Type orderType,
                           Multimap<Resource, RuntimeSpecification> requests) {
    this.orderType = orderType;
    this.requests = requests.asMap().entrySet().iterator();
  }

  WeaveSpecification.Order.Type getOrderType() {
    return orderType;
  }

  /**
   * Remove a resource request and return it.
   * @return The {@link Resource} and {@link Collection} of {@link RuntimeSpecification} or
   *         {@code null} if there is no more request.
   */
  Map.Entry<Resource, ? extends Collection<RuntimeSpecification>> takeRequest() {
    Map.Entry<Resource, Collection<RuntimeSpecification>> next = Iterators.getNext(requests, null);
    return next == null ? null : Maps.immutableEntry(next.getKey(), ImmutableList.copyOf(next.getValue()));
  }
}
