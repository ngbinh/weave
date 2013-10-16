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

import com.continuuity.weave.api.SecureStore;
import org.apache.hadoop.security.Credentials;

/**
 * A {@link SecureStore} for hadoop credentials.
 */
public final class YarnSecureStore implements SecureStore {

  private final Credentials credentials;

  public static SecureStore create() {
    return create(new Credentials());
  }

  public static SecureStore create(Credentials credentials) {
    return new YarnSecureStore(credentials);
  }

  private YarnSecureStore(Credentials credentials) {
    this.credentials = credentials;
  }

  @Override
  public <T> T getStore() {
    return (T) credentials;
  }
}
