package com.continuuity.weave.internal;

import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.internal.state.Message;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A {@link ServiceController} that allows sending a message directly. Internal use only.
 */
public interface WeaveContainerController extends ServiceController {

  ListenableFuture<Message> sendMessage(Message message);

  /**
   * Calls to indicated that the container that this controller is associated with is completed.
   * Any resources it hold will be releases and all pending futures will be cancelled.
   */
  void completed(int exitStatus);
}
