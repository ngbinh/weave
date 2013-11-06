/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.SecureStore;

/**
 *
 */
public interface SecureStoreUpdater {

  /**
   * Invoked when an update to SecureStore is needed.
   *
   * @param application The name of the application.
   * @param runId The runId of the live application.
   * @return A new {@link SecureStore}.
   */
  SecureStore update(String application, RunId runId);
}
