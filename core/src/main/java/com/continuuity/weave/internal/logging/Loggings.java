/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class Loggings {

  public static void forceFlush() {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();

    if (loggerFactory instanceof LoggerContext) {
      Appender<ILoggingEvent> appender = ((LoggerContext) loggerFactory).getLogger(Logger.ROOT_LOGGER_NAME)
                                                                        .getAppender("KAFKA");
      if (appender != null && appender instanceof KafkaAppender) {
        ((KafkaAppender) appender).forceFlush();
      }
    }
  }

  private Loggings() {
  }
}
