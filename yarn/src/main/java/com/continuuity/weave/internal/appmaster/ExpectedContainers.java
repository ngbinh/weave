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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * This class hold information about the expected container count for each runnable. It also
 * keep track of the timestamp where the expected count has been updated.
 */
final class ExpectedContainers {

  private final Map<String, ExpectedCount> expectedCounts;

  ExpectedContainers(Map<String, Integer> expected) {
    expectedCounts = Maps.newHashMap();
    long now = System.currentTimeMillis();

    for (Map.Entry<String, Integer> entry : expected.entrySet()) {
      expectedCounts.put(entry.getKey(), new ExpectedCount(entry.getValue(), now));
    }
  }

  synchronized void setExpected(String runnable, int expected) {
    expectedCounts.put(runnable, new ExpectedCount(expected, System.currentTimeMillis()));
  }

  /**
   * Updates the ExpectCount timestamp to current time if the running count is the same as expected count.
   * @param runnable Name of runnable
   * @param running Number of running instances.
   * @return {@code true} if the running count match with expected count, {@code false} otherwise.
   */
  synchronized boolean sameAsExpected(String runnable, int running) {
    if (expectedCounts.get(runnable).getCount() == running) {
      expectedCounts.put(runnable, new ExpectedCount(running, System.currentTimeMillis()));
      return true;
    }
    return false;
  }

  synchronized int getExpected(String runnable) {
    return expectedCounts.get(runnable).getCount();
  }

  synchronized Map<String, ExpectedCount> getAll() {
    return ImmutableMap.copyOf(expectedCounts);
  }

  static final class ExpectedCount {
    private final int count;
    private final long timestamp;

    private ExpectedCount(int count, long timestamp) {
      this.count = count;
      this.timestamp = timestamp;
    }

    int getCount() {
      return count;
    }

    long getTimestamp() {
      return timestamp;
    }
  }
}
