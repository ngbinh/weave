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

import com.continuuity.weave.common.Threads;
import com.continuuity.weave.internal.logging.KafkaAppender;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.StringReader;
import java.util.concurrent.ExecutionException;

/**
 * Class for main method that starts a service.
 */
public abstract class ServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceMain.class);

  protected final void doMain(final Service service) throws ExecutionException, InterruptedException {
    configureLogger();

    final String serviceName = service.toString();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (service.isRunning()) {
          LOG.info("Shutdown hook triggered. Shutting down service " + serviceName);
          service.stopAndWait();
        }
      }
    });

    // Listener for state changes of the service
    final SettableFuture<Service.State> completion = SettableFuture.create();
    service.addListener(new Service.Listener() {
      @Override
      public void starting() {
        LOG.info("Starting service " + serviceName);
      }

      @Override
      public void running() {
        LOG.info("Service running " + serviceName);
      }

      @Override
      public void stopping(Service.State from) {
        LOG.info("Stopping service " + serviceName + " from " + from);
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Service terminated " + serviceName + " from " + from);
        completion.set(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Service failure " + serviceName, failure);
        completion.setException(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Starts the service
    service.start();

    try {
      // If container failed with exception, the future.get() will throws exception
      completion.get();
    } finally {
      ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
      if (loggerFactory instanceof LoggerContext) {
        ((LoggerContext) loggerFactory).stop();
      }
    }
  }

  protected abstract String getHostname();

  protected abstract String getKafkaZKConnect();

  private void configureLogger() {
    // Check if SLF4J is bound to logback in the current environment
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      return;
    }

    LoggerContext context = (LoggerContext) loggerFactory;
    context.reset();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);

    try {
      File weaveLogback = new File("logback-template.xml");
      if (weaveLogback.exists()) {
        configurator.doConfigure(weaveLogback);
      }
      new ContextInitializer(context).autoConfig();
    } catch (JoranException e) {
      throw Throwables.propagate(e);
    }
    doConfigure(configurator, getLogConfig(getLoggerLevel(context.getLogger(Logger.ROOT_LOGGER_NAME))));
  }

  private void doConfigure(JoranConfigurator configurator, String config) {
    try {
      configurator.doConfigure(new InputSource(new StringReader(config)));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getLogConfig(String rootLevel) {
    return
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<configuration>\n" +
      "    <appender name=\"KAFKA\" class=\"" + KafkaAppender.class.getName() + "\">\n" +
      "        <topic>log</topic>\n" +
      "        <hostname>" + getHostname() + "</hostname>\n" +
      "        <zookeeper>" + getKafkaZKConnect() + "</zookeeper>\n" +
      "    </appender>\n" +
      "    <logger name=\"com.continuuity.weave.internal.logging\" additivity=\"false\" />\n" +
      "    <root level=\"" + rootLevel + "\">\n" +
      "        <appender-ref ref=\"KAFKA\"/>\n" +
      "    </root>\n" +
      "</configuration>";
  }

  private String getLoggerLevel(Logger logger) {
    if (logger instanceof ch.qos.logback.classic.Logger) {
      return ((ch.qos.logback.classic.Logger) logger).getLevel().toString();
    }

    if (logger.isTraceEnabled()) {
      return "TRACE";
    }
    if (logger.isDebugEnabled()) {
      return "DEBUG";
    }
    if (logger.isInfoEnabled()) {
      return "INFO";
    }
    if (logger.isWarnEnabled()) {
      return "WARN";
    }
    if (logger.isErrorEnabled()) {
      return "ERROR";
    }
    return "OFF";
  }
}
