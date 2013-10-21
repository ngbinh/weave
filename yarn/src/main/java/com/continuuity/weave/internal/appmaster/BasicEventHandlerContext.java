/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.EventHandlerContext;
import com.continuuity.weave.api.EventHandlerSpecification;

/**
 *
 */
final class BasicEventHandlerContext implements EventHandlerContext {

  private final EventHandlerSpecification specification;

  BasicEventHandlerContext(EventHandlerSpecification specification) {
    this.specification = specification;
  }

  @Override
  public EventHandlerSpecification getSpecification() {
    return specification;
  }
}
