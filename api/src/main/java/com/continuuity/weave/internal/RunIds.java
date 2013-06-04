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

import com.continuuity.weave.api.RunId;
import com.google.common.base.Preconditions;

import java.util.UUID;

/**
 * Factory class for creating instance of {@link RunId}.
 */
public final class RunIds {

  public static RunId generate() {
    return new RunIdImpl(UUID.randomUUID().toString());
  }

  public static RunId fromString(String str) {
    return new RunIdImpl(str);
  }

  private RunIds() {
  }

  private static final class RunIdImpl implements RunId {

    final String id;

    private RunIdImpl(String id) {
      Preconditions.checkArgument(id != null, "RunId cannot be null.");
      this.id = id;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String toString() {
      return getId();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || !(other instanceof RunId)) {
        return false;
      }
      return id.equals(((RunId)other).getId());
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }
  }
}
