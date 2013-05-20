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

import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * This class builds jar files based on class dependencies.
 */
public final class ApplicationBundler {

  private final List<String> excludePackages;
  private final List<String> includePackages;
  private final Set<String> acceptClassPath;

  /**
   * Constructs a ApplicationBundler.
   *
   * @param excludePackages Class packages to exclude
   * @param includePackages Always includes classes/resources in these packages. Note that
   *                        classes in these packages will not get inspected for dependencies.
   */
  public ApplicationBundler(Iterable<String> excludePackages, Iterable<String> includePackages) {
    this.excludePackages = ImmutableList.copyOf(excludePackages);
    this.includePackages = ImmutableList.copyOf(includePackages);
    this.acceptClassPath = Sets.difference(Sets.newHashSet(Splitter.on(File.pathSeparatorChar)
                                                             .split(System.getProperty("java.class.path"))),
                                           Sets.newHashSet(Splitter.on(File.pathSeparatorChar)
                                                             .split(System.getProperty("sun.boot.class.path"))));
  }

  /**
   * Creates a jar file which includes all the given classes and all the classes that they depended on.
   * The jar will also include all classes and resources under the packages as given as include packages
   * in the constructor.
   *
   * @param target Where to save the target jar file.
   * @param classes Set of classes to start the dependency traversal.
   * @throws IOException
   */
  public void createBundle(Location target, Iterable<Class<?>> classes) throws IOException {
    // Write the jar to local tmp file first
    File tmpJar = File.createTempFile(target.getName(), ".tmp");
    try {
      Set<String> entries = Sets.newHashSet();
      JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(tmpJar));
      try {
        // Add all classes under the packages as listed in includePackages
        for (String pkg : includePackages) {
          copyPackage(pkg, entries, jarOut);
        }
        findDependencies(classes, entries, jarOut);
      } finally {
        jarOut.close();
      }
      // Copy the tmp jar into destination.
      OutputStream os = new BufferedOutputStream(target.getOutputStream());
      try {
        Files.copy(tmpJar, os);
      } finally {
        os.close();
      }
    } finally {
      tmpJar.delete();
    }
  }

  /**
   * Same as calling {@link #createBundle(Location, Iterable)}.
   */
  public void createBundle(Location target, Class<?> clz, Class<?>...classes) throws IOException {
    createBundle(target, ImmutableSet.<Class<?>>builder().add(clz).add(classes).build());
  }

  private void findDependencies(Iterable<Class<?>> classes,
                                final Set<String> entries, final JarOutputStream jarOut) throws IOException {

    Iterable<String> classNames = Iterables.transform(classes, new Function<Class<?>, String>() {
      @Override
      public String apply(Class<?> input) {
        return input.getName();
      }
    });
    Dependencies.findClassDependencies(Thread.currentThread().getContextClassLoader(), acceptClassPath, classNames,
                                       new Dependencies.ClassAcceptor() {
      @Override
      public boolean accept(String className, byte[] bytecode) {
        for (String exclude : excludePackages) {
          if (className.startsWith(exclude)) {
            return false;
          }
        }
        putDirEntry(className.substring(0, className.lastIndexOf('.')), entries, jarOut);
        putClassEntry(className, bytecode, entries, jarOut);

        return true;
      }
    });
  }

  /**
   * Saves a directory entry to the jar output.
   */
  private void putDirEntry(String pkg, Set<String> entries, JarOutputStream jarOut) {
    try {
      String entry = "";
      for (String dir : Splitter.on('.').split(pkg)) {
        entry += dir + '/';
        if (entries.add(entry)) {
          jarOut.putNextEntry(new JarEntry(entry));
          jarOut.closeEntry();
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Saves a class entry to the jar output.
   */
  private void putClassEntry(String className, byte[] bytecode, Set<String> entries, JarOutputStream jarOut) {
    try {
      String entry = className.replace('.', '/') + ".class";
      if (entries.add(entry)) {
        jarOut.putNextEntry(new JarEntry(entry));
        jarOut.write(bytecode);
        jarOut.closeEntry();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Copies everything under the given package to the jar output.
   */
  private void copyPackage(String pkg, Set<String> entries, JarOutputStream jarOut) throws IOException {
    String prefix = pkg.replace('.', '/');

    Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(prefix);
    while (resources.hasMoreElements()) {
      URL url = resources.nextElement();
      if ("jar".equals(url.getProtocol())) {
        copyJarEntries(url, entries, jarOut);
      } else if ("file".equals(url.getProtocol())) {
        String path = url.getFile();
        File baseDir = new File(path.substring(0, path.indexOf(prefix)));
        copyFileEntries(baseDir, prefix, entries, jarOut);
      }
      // Otherwise, ignore the package
    }
  }

  /**
   * Copies all entries under the path specified by the jarPath to the JarOutputStream.
   */
  private void copyJarEntries(URL jarPath, Set<String> entries, JarOutputStream jarOut) throws IOException {
    String spec = new URL(jarPath.getFile()).getFile();
    String entryRoot = spec.substring(spec.lastIndexOf('!') + 1);
    if (entryRoot.charAt(0) == '/') {
      entryRoot = entryRoot.substring(1);
    }
    JarFile jarFile = new JarFile(spec.substring(0, spec.lastIndexOf('!')));

    try {
      Enumeration<JarEntry> jarEntries = jarFile.entries();
      while (jarEntries.hasMoreElements()) {
        JarEntry jarEntry = jarEntries.nextElement();
        String name = jarEntry.getName();
        if (name == null || !name.startsWith(entryRoot)) {
          continue;
        }
        if (entries.add(name)) {
          jarOut.putNextEntry(new JarEntry(jarEntry));
          ByteStreams.copy(jarFile.getInputStream(jarEntry), jarOut);
          jarOut.closeEntry();
        }
      }
    } finally {
      jarFile.close();
    }
  }

  /**
   * Copies all entries under the file path.
   */
  private void copyFileEntries(File baseDir, String entryRoot,
                               Set<String> entries, JarOutputStream jarOut) throws IOException {
    URI baseUri = baseDir.toURI();
    File packageFile = new File(baseDir, entryRoot);
    Queue<File> queue = Lists.newLinkedList();
    queue.add(packageFile);
    while (!queue.isEmpty()) {
      File file = queue.remove();

      String entry = baseUri.relativize(file.toURI()).getPath();
      if (entries.add(entry)) {
        jarOut.putNextEntry(new JarEntry(entry));
        if (file.isFile()) {
          Files.copy(file, jarOut);
        }
        jarOut.closeEntry();
      }

      if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files != null) {
          queue.addAll(Arrays.asList(files));
        }
      }
    }
  }
}
