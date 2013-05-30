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
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.internal.ApplicationBundler;
import com.continuuity.weave.internal.DefaultLocalFile;
import com.continuuity.weave.internal.DefaultRuntimeSpecification;
import com.continuuity.weave.internal.DefaultWeaveSpecification;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.RunIds;
import com.continuuity.weave.internal.WeaveContainerMain;
import com.continuuity.weave.internal.appmaster.ApplicationMasterMain;
import com.continuuity.weave.internal.json.LocalFileCodec;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.continuuity.weave.launcher.WeaveLauncher;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Implementation for {@link WeavePreparer} to prepare and launch distributed application on Hadoop YARN.
 */
final class YarnWeavePreparer implements WeavePreparer {

  private static final Logger LOG = LoggerFactory.getLogger(YarnWeavePreparer.class);
  private static final String KAFKA_ARCHIVE = "kafka-0.7.2.tgz";
  private static final int APP_MASTER_MEMORY_MB = 1024;

  private final WeaveSpecification weaveSpec;
  private final YarnClient yarnClient;
  private final ZKClient zkClient;
  private final LocationFactory locationFactory;
  private final RunId runId;

  private final List<LogHandler> logHandlers = Lists.newArrayList();
  private final List<String> arguments = Lists.newArrayList();
  private final Set<Class<?>> dependencies = Sets.newIdentityHashSet();
  private final List<URI> resources = Lists.newArrayList();
  private final List<String> classPaths = Lists.newArrayList();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();

  YarnWeavePreparer(WeaveSpecification weaveSpec, YarnClient yarnClient,
                    ZKClient zkClient, LocationFactory locationFactory) {
    this.weaveSpec = weaveSpec;
    this.yarnClient = yarnClient;
    this.zkClient = ZKClients.namespace(zkClient, "/" + weaveSpec.getName());
    this.locationFactory = locationFactory;
    this.runId = RunIds.generate();
  }

  @Override
  public WeavePreparer addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    return this;
  }

  @Override
  public WeavePreparer withApplicationArguments(String... args) {
    return withApplicationArguments(ImmutableList.copyOf(args));
  }

  @Override
  public WeavePreparer withApplicationArguments(Iterable<String> args) {
    Iterables.addAll(arguments, args);
    return this;
  }

  @Override
  public WeavePreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, ImmutableList.copyOf(args));
  }

  @Override
  public WeavePreparer withArguments(String runnableName, Iterable<String> args) {
    runnableArgs.putAll(runnableName, args);
    return this;
  }

  @Override
  public WeavePreparer withDependencies(Class<?>... classes) {
    return withDependencies(ImmutableList.copyOf(classes));
  }

  @Override
  public WeavePreparer withDependencies(Iterable<Class<?>> classes) {
    Iterables.addAll(dependencies, classes);
    return this;
  }

  @Override
  public WeavePreparer withResources(URI... resources) {
    return withResources(ImmutableList.copyOf(resources));
  }

  @Override
  public WeavePreparer withResources(Iterable<URI> resources) {
    Iterables.addAll(this.resources, resources);
    return this;
  }

  @Override
  public WeavePreparer withClassPaths(String... classPaths) {
    return withClassPaths(ImmutableList.copyOf(classPaths));
  }

  @Override
  public WeavePreparer withClassPaths(Iterable<String> classPaths) {
    Iterables.addAll(this.classPaths, classPaths);
    return this;
  }

  @Override
  public WeaveController start() {
    // TODO: Unify this with {@link ProcessLauncher}
    try {
      GetNewApplicationResponse response = yarnClient.getNewApplication();

      ApplicationSubmissionContext appSubmissionContext = Records.newRecord(ApplicationSubmissionContext.class);
      appSubmissionContext.setApplicationId(response.getApplicationId());
      appSubmissionContext.setApplicationName(weaveSpec.getName());

      Map<String, LocalResource> localResources = Maps.newHashMap();

      Multimap<String, LocalFile> transformedLocalFiles = HashMultimap.create();

      createAppMasterJar(createBundler(), localResources);
      createContainerJar(createBundler(), localResources);
      populateRunnableResources(weaveSpec, transformedLocalFiles);
      saveWeaveSpec(weaveSpec, transformedLocalFiles, localResources);
      saveLogback(localResources);
      saveLauncher(localResources);
      saveKafka(localResources);
      saveLocalFiles(localResources, ImmutableSet.of("weaveSpec.json",
                                                     "logback-template.xml",
                                                     "container.jar",
                                                     "launcher.jar"));

      ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
      containerLaunchContext.setLocalResources(localResources);

      // java -cp launcher.jar:$HADOOP_CONF_DIR -XmxMemory
      //     com.continuuity.weave.internal.WeaveLauncher
      //     appMaster.jar
      //     com.continuuity.weave.internal.appmaster.ApplicationMasterMain
      //     false
      containerLaunchContext.setCommands(ImmutableList.of(
        "java",
        "-cp", "launcher.jar:$HADOOP_CONF_DIR",
        "-Xmx" + APP_MASTER_MEMORY_MB + "m",
        WeaveLauncher.class.getName(),
        "appMaster.jar",
        ApplicationMasterMain.class.getName(),
        Boolean.FALSE.toString(),
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
      ));

      containerLaunchContext.setEnvironment(ImmutableMap.<String, String>builder()
        .put(EnvKeys.WEAVE_APP_ID, Integer.toString(response.getApplicationId().getId()))
        .put(EnvKeys.WEAVE_APP_ID_CLUSTER_TIME, Long.toString(response.getApplicationId().getClusterTimestamp()))
        .put(EnvKeys.WEAVE_ZK_CONNECT, zkClient.getConnectString())
        .put(EnvKeys.WEAVE_APPLICATION_ARGS, encodeArguments(arguments))
        .put(EnvKeys.WEAVE_RUNNABLE_ARGS, encodeRunnableArguments(runnableArgs))
        .put(EnvKeys.WEAVE_RUN_ID, runId.getId())
        .build()
      );
      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(APP_MASTER_MEMORY_MB);
      containerLaunchContext.setResource(capability);

      appSubmissionContext.setAMContainerSpec(containerLaunchContext);

      yarnClient.submitApplication(appSubmissionContext);

      return createController(runId, logHandlers);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private WeaveController createController(RunId runId, Iterable<LogHandler> logHandlers) {
    YarnWeaveController controller = new YarnWeaveController(yarnClient, zkClient, runId, logHandlers);
    controller.start();
    return controller;
  }

  private String encodeArguments(List<String> args) {
    return new Gson().toJson(args);
  }

  private String encodeRunnableArguments(Multimap<String, String> args) {
    return new Gson().toJson(args.asMap());
  }

  private ApplicationBundler createBundler() {
    return new ApplicationBundler(ImmutableList.<String>of());

  }

  private void createAppMasterJar(ApplicationBundler bundler,
                                  Map<String, LocalResource> localResources) throws IOException {
    LOG.debug("Create and copy appMaster.jar");
    Location location = createTempLocation("appMaster", ".jar");
    bundler.createBundle(location, ApplicationMasterMain.class);
    LOG.debug("Done appMaster.jar");

    localResources.put("appMaster.jar", YarnUtils.createLocalResource(location));
  }

  private void createContainerJar(ApplicationBundler bundler,
                                  Map<String, LocalResource> localResources) throws IOException {
    try {
      Set<Class<?>> classes = Sets.newIdentityHashSet();
      classes.add(WeaveContainerMain.class);
      classes.addAll(dependencies);

      ClassLoader classLoader = getClass().getClassLoader();
      for (RuntimeSpecification spec : weaveSpec.getRunnables().values()) {
        classes.add(classLoader.loadClass(spec.getRunnableSpecification().getClassName()));
      }

      LOG.debug("Create and copy container.jar");
      Location location = createTempLocation("container", ".jar");
      bundler.createBundle(location, classes, resources);
      LOG.debug("Done container.jar");

      localResources.put("container.jar", YarnUtils.createLocalResource(location));

    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Based on the given {@link WeaveSpecification}, upload LocalFiles to Yarn Cluster.
   * @param weaveSpec The {@link WeaveSpecification} for populating resource.
   * @param localFiles A Multimap to store runnable name to transformed LocalFiles.
   * @throws IOException
   */
  private void populateRunnableResources(WeaveSpecification weaveSpec,
                                         Multimap<String, LocalFile> localFiles) throws IOException {

    LOG.debug("Populating Runnable LocalFiles");
    for (Map.Entry<String, RuntimeSpecification> entry: weaveSpec.getRunnables().entrySet()) {
      String name = entry.getKey();
      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        Location location;

        URI uri = localFile.getURI();
        if ("hdfs".equals(uri.getScheme())) {
          // Assuming the location factory is HDFS one. If it is not, it will failed, which is the correct behavior.
          location = locationFactory.create(uri);
        } else {
          URL url = uri.toURL();
          LOG.debug("Create and copy {} : {}", name, url);
          // Temp file suffix is repeated with the file name to preserve original suffix for expansion.
          String path = url.getFile();
          location = copyFromURL(url, createTempLocation(localFile.getName(),
                                                         path.substring(path.lastIndexOf('/') + 1)));
          LOG.debug("Done {} : {}", name, url);
        }

        localFiles.put(name, new DefaultLocalFile(localFile.getName(), location.toURI(), location.lastModified(),
                                                  location.length(), localFile.isArchive(), localFile.getPattern()));
      }
    }
    LOG.debug("Done Runnable LocalFiles");
  }

  private void saveWeaveSpec(WeaveSpecification spec, final Multimap<String, LocalFile> localFiles,
                             Map<String, LocalResource> localResources) throws IOException {
    // Rewrite LocalFiles inside weaveSpec
    Map<String, RuntimeSpecification> runtimeSpec = Maps.transformEntries(
      spec.getRunnables(), new Maps.EntryTransformer<String, RuntimeSpecification, RuntimeSpecification>() {
      @Override
      public RuntimeSpecification transformEntry(String key, RuntimeSpecification value) {
        return new DefaultRuntimeSpecification(value.getName(), value.getRunnableSpecification(),
                                               value.getResourceSpecification(), localFiles.get(key));
      }
    });

    // Serialize into a local temp file.
    LOG.debug("Create and copy weaveSpec.json");
    Location location = createTempLocation("weaveSpec", ".json");
    Writer writer = new OutputStreamWriter(location.getOutputStream(), Charsets.UTF_8);
    try {
      WeaveSpecificationAdapter.create().toJson(new DefaultWeaveSpecification(spec.getName(), runtimeSpec,
                                                                              spec.getOrders()), writer);
    } finally {
      writer.close();
    }
    LOG.debug("Done weaveSpec.json");
    localResources.put("weaveSpec.json", YarnUtils.createLocalResource(location));
  }

  private void saveLogback(Map<String, LocalResource> localResources) throws IOException {
    LOG.debug("Create and copy logback-template.xml");
    Location location = copyFromURL(getClass().getClassLoader().getResource("logback-template.xml"),
                                    createTempLocation("logback-template", ".xml"));
    LOG.debug("Done logback-template.xml");
    localResources.put("logback-template.xml", YarnUtils.createLocalResource(location));
  }

  /**
   * Creates the launcher.jar.
   */
  private void saveLauncher(Map<String, LocalResource> localResources) throws URISyntaxException, IOException {

    LOG.debug("Create and copy launcher.jar");
    Location location = createTempLocation("launcher", ".jar");

    // Create a jar file with the WeaveLauncher optionally a json serialized classpath.json in it.
    String launcherName = WeaveLauncher.class.getName().replace('.', '/') + ".class";
    JarOutputStream jarOut = new JarOutputStream(location.getOutputStream());
    try {
      jarOut.putNextEntry(new JarEntry(launcherName));
      InputStream is = getClass().getClassLoader().getResourceAsStream(launcherName);
      try {
        ByteStreams.copy(is, jarOut);
      } finally {
        is.close();
      }

      if (!classPaths.isEmpty()) {
        jarOut.putNextEntry(new JarEntry("classpath"));
        jarOut.write(Joiner.on(':').join(classPaths).getBytes(Charsets.UTF_8));
      }
    } finally {
      jarOut.close();
    }
    LOG.debug("Done launcher.jar");
    localResources.put("launcher.jar", YarnUtils.createLocalResource(location));
  }

  private void saveKafka(Map<String, LocalResource> localResources) throws IOException {
    LOG.debug("Copy kafka.tgz");
    Location location = copyFromURL(getClass().getClassLoader().getResource(KAFKA_ARCHIVE),
                                    createTempLocation("kafka", ".tgz"));
    LOG.debug("Done kafka.tgz");
    LocalResource localResource = YarnUtils.createLocalResource(location);
    localResource.setType(LocalResourceType.ARCHIVE);
    localResources.put("kafka.tgz", localResource);
  }


  private void saveLocalFiles(Map<String, LocalResource> localResources, Set<String> keys) throws IOException {
    Map<String, LocalFile> localFiles = Maps.transformEntries(
      Maps.filterKeys(localResources, Predicates.in(keys)),
      new Maps.EntryTransformer<String, LocalResource, LocalFile>() {
      @Override
      public LocalFile transformEntry(String key, LocalResource value) {
        try {
          return new DefaultLocalFile(key, ConverterUtils.getPathFromYarnURL(value.getResource()).toUri(),
                                      value.getTimestamp(), value.getSize(),
                                      value.getType() != LocalResourceType.FILE, value.getPattern());
        } catch (URISyntaxException e) {
          throw Throwables.propagate(e);
        }
      }
    });

    LOG.debug("Create and copy localFiles.json");
    Location location = createTempLocation("localFiles", ".json");
    Writer writer = new OutputStreamWriter(location.getOutputStream(), Charsets.UTF_8);
    try {
      new GsonBuilder().registerTypeAdapter(LocalFile.class, new LocalFileCodec())
        .create().toJson(localFiles.values(), new TypeToken<List<LocalFile>>() {
      }.getType(), writer);
    } finally {
      writer.close();
    }
    LOG.debug("Done localFiles.json");
    localResources.put("localFiles.json", YarnUtils.createLocalResource(location));
  }

  private Location copyFromURL(URL url, Location target) throws IOException {
    InputStream is = url.openStream();
    try {
      OutputStream os = new BufferedOutputStream(target.getOutputStream());
      try {
        ByteStreams.copy(is, os);
      } finally {
        os.close();
      }
    } finally {
      is.close();
    }
    return target;
  }

  private Location createTempLocation(String path, String suffix) {
    try {
      return locationFactory.create(String.format("/%s/%s/%s",
                                                  weaveSpec.getName(), runId.getId(), path)).getTempFile(suffix);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
