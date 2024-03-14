/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.util.core.jvm;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;

public class JarUtil {
    public static Class loadFromJar(final String classname, final Path jarfile) {

        try {
            URLClassLoader jarClassLoader = new URLClassLoader(new URL[]{jarfile.toUri().toURL()}, Thread.currentThread().getContextClassLoader());
            return jarClassLoader.loadClass(classname);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
