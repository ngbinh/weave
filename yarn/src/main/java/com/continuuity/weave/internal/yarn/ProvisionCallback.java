/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.internal.ProcessLauncher;

/**
 *
 */
public interface ProvisionCallback {

  /**
   * Called when provision request is completed. The {@link ProcessLauncher} is for
   * launching executable in the new container. If nothing is launched through the given {@link ProcessLauncher}
   * when this method return, the provisioned container will be released back to the cluster.
   *
   * @param launcher for launching executable.
   */
  void onComplete(ProcessLauncher launcher);
}
