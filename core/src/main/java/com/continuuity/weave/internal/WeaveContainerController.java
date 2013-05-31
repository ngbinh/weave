package com.continuuity.weave.internal;

import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.internal.state.Message;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A {@link ServiceController} that allows sending a message directly. Internal use only.
 */
public interface WeaveContainerController extends ServiceController {

  ListenableFuture<Message> sendMessage(Message message);
}
