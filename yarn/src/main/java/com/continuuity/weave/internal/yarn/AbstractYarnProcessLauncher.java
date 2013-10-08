/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.internal.ProcessController;
import com.continuuity.weave.internal.ProcessLauncher;
import com.continuuity.weave.internal.utils.Paths;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;

/**
 * Abstract class to help creating different types of process launcher that process on yarn.
 *
 * @param <T> Type of the object that contains information about the container that the process is going to launch.
 */
public abstract class AbstractYarnProcessLauncher<T> implements ProcessLauncher<T> {

  private final T containerInfo;

  protected AbstractYarnProcessLauncher(T containerInfo) {
    this.containerInfo = containerInfo;
  }

  @Override
  public T getContainerInfo() {
    return containerInfo;
  }

  @Override
  public PrepareLaunchContext prepareLaunch(Map<String, String> environments, Iterable<LocalFile> resources) {
    return new PrepareLaunchContextImpl(environments, resources);
  }

  /**
   * Tells whether to append suffix to localize resource name for archive file type. Default is true.
   */
  protected boolean useArchiveSuffix() {
    return true;
  }

  /**
   * For children class to override to perform actual process launching.
   */
  protected abstract <R> ProcessController<R> doLaunch(ContainerLaunchContext launchContext);

  /**
   * Implementation for the {@link PrepareLaunchContext}.
   */
  private final class PrepareLaunchContextImpl implements PrepareLaunchContext {

    private final ContainerLaunchContext launchContext;
    private final Map<String, LocalResource> localResources;
    private final Map<String, String> environment;
    private final List<String> commands;

    private PrepareLaunchContextImpl(Map<String, String> env, Iterable<LocalFile> localFiles) {
      this.launchContext = Records.newRecord(ContainerLaunchContext.class);
      this.localResources = Maps.newHashMap();
      this.environment = Maps.newHashMap(env);
      this.commands = Lists.newLinkedList();

      for (LocalFile localFile : localFiles) {
        addLocalFile(localFile);
      }
    }

    private void addLocalFile(LocalFile localFile) {
      String name = localFile.getName();
      // Always append the file extension as the resource name so that archive expansion by Yarn could work.
      // Renaming would happen by the Container Launcher.
      if (localFile.isArchive() && useArchiveSuffix()) {
        String path = localFile.getURI().toString();
        String suffix = Paths.getExtension(path);
        if (!suffix.isEmpty()) {
          name += '.' + suffix;
        }
      }
      localResources.put(name, YarnUtils.createLocalResource(localFile));
    }

    @Override
    public ResourcesAdder withResources() {
      return new MoreResourcesImpl();
    }

    @Override
    public AfterResources noResources() {
      return new MoreResourcesImpl();
    }

    private final class MoreResourcesImpl implements MoreResources {

      @Override
      public MoreResources add(LocalFile localFile) {
        addLocalFile(localFile);
        return this;
      }

      @Override
      public EnvironmentAdder withEnvironment() {
        return finish();
      }

      @Override
      public AfterEnvironment noEnvironment() {
        return finish();
      }

      private MoreEnvironmentImpl finish() {
        launchContext.setLocalResources(localResources);
        return new MoreEnvironmentImpl();
      }
    }

    private final class MoreEnvironmentImpl implements MoreEnvironment {

      @Override
      public CommandAdder withCommands() {
        launchContext.setEnvironment(environment);
        return new MoreCommandImpl();
      }

      @Override
      public <V> MoreEnvironment add(String key, V value) {
        environment.put(key, value.toString());
        return this;
      }
    }

    private final class MoreCommandImpl implements MoreCommand, StdOutSetter, StdErrSetter {

      private final StringBuilder commandBuilder = new StringBuilder();

      @Override
      public StdOutSetter add(String cmd, String... args) {
        commandBuilder.append(cmd);
        for (String arg : args) {
          commandBuilder.append(' ').append(arg);
        }
        return this;
      }

      @Override
      public <R> ProcessController<R> launch() {
        launchContext.setCommands(commands);
        return doLaunch(launchContext);
      }

      @Override
      public MoreCommand redirectError(String stderr) {
        commandBuilder.append(' ').append("2>").append(stderr);
        return noError();
      }

      @Override
      public MoreCommand noError() {
        commands.add(commandBuilder.toString());
        commandBuilder.setLength(0);
        return this;
      }

      @Override
      public StdErrSetter redirectOutput(String stdout) {
        commandBuilder.append(' ').append("1>").append(stdout);
        return this;
      }

      @Override
      public StdErrSetter noOutput() {
        return this;
      }
    }
  }
}
