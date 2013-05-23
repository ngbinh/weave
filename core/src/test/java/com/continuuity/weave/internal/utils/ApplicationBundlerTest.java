/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.utils;

import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.internal.ApplicationBundler;
import com.continuuity.weave.internal.WeaveContainerMain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 *
 */
public class ApplicationBundlerTest {

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Test
  public void testFindDependencies() throws IOException, ClassNotFoundException {
    Location location = new LocalLocationFactory(tmpDir.newFolder()).create("test.jar");

    // Create a jar file with by tracing dependency
    ApplicationBundler bundler = new ApplicationBundler(ImmutableList.<String>of());
    bundler.createBundle(location, WeaveContainerMain.class);

    File targetDir = tmpDir.newFolder();
    unjar(new File(location.toURI()), targetDir);

    // Load the class back, it should be loaded by the custom classloader
    ClassLoader classLoader = createClassLoader(targetDir);
    Class<?> clz = classLoader.loadClass(WeaveContainerMain.class.getName());
    Assert.assertSame(classLoader, clz.getClassLoader());

    // For system classes, they shouldn't be packaged, hence loaded by different classloader.
    clz = classLoader.loadClass(Object.class.getName());
    Assert.assertNotSame(classLoader, clz.getClassLoader());
  }

  private void unjar(File jarFile, File targetDir) throws IOException {
    JarInputStream jarInput = new JarInputStream(new FileInputStream(jarFile));
    try {
      JarEntry jarEntry = jarInput.getNextJarEntry();
      while (jarEntry != null) {
        File target = new File(targetDir, jarEntry.getName());
        if (jarEntry.isDirectory()) {
          target.mkdirs();
        } else {
          target.getParentFile().mkdirs();
          ByteStreams.copy(jarInput, Files.newOutputStreamSupplier(target));
        }

        jarEntry = jarInput.getNextJarEntry();
      }
    } finally {
      jarInput.close();
    }
  }

  private ClassLoader createClassLoader(File dir) throws MalformedURLException {
    List<URL> urls = Lists.newArrayList();
    urls.add(new File(dir, "classes").toURI().toURL());
    File[] libFiles = new File(dir, "lib").listFiles();
    if (libFiles != null) {
      for (File file : libFiles) {
        urls.add(file.toURI().toURL());
      }
    }
    return new URLClassLoader(urls.toArray(new URL[0])) {
      @Override
      protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // Load class from the given URLs first before delegating to parent.
        try {
          return super.findClass(name);
        } catch (ClassNotFoundException e) {
          ClassLoader parent = getParent();
          return parent == null ? ClassLoader.getSystemClassLoader().loadClass(name) : parent.loadClass(name);
        }
      }
    };
  }
}
