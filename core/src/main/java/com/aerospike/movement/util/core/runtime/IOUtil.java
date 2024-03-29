/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.runtime;



import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class IOUtil {
    private IOUtil() {
    }

    public static ForkJoinTask<Object> backgroundTicker(final double ticksPerSecond, final Runnable runnable) {
        return (ForkJoinTask<Object>) new ForkJoinPool(1).submit(() -> {
            try {
                Thread.sleep((long) (TimeUnit.SECONDS.toMillis(1) / ticksPerSecond));
            } catch (InterruptedException e) {
                throw RuntimeUtil.getErrorHandler(e).handleError(e);
            }
            runnable.run();
        });
    }

    public static String readFromResources(final String resourceName) {
        try (InputStream is = IOUtil.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (is == null)
                throw RuntimeUtil.getErrorHandler(IOUtil.class).handleError(new RuntimeException("Could not find resource: " + resourceName));
            return new String(is.readAllBytes());
        } catch (IOException e) {
            throw RuntimeUtil.getErrorHandler(e).handleError(e);
        }
    }

    public static File copyFromResourcesIntoNewTempFile(final String resourceName) {
        return copyFromResourcesIntoNewTempFile(resourceName, resourceName);
    }

    public static File copyFromResourcesIntoNewTempFile(final String resourcePath, final String tempFileName) {
        try {
            final File tempFile = File.createTempFile("resource", tempFileName);
            tempFile.deleteOnExit();
            try (InputStream is = IOUtil.class.getClassLoader().getResourceAsStream(resourcePath);
                 FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
                byte dataBuffer[] = new byte[1024];
                int bytesRead;
                while ((bytesRead = is.read(dataBuffer, 0, 1024)) != -1) {
                    fileOutputStream.write(dataBuffer, 0, bytesRead);
                }
            }
            return tempFile;
        } catch (IOException e) {
            throw RuntimeUtil.getErrorHandler(e).handleError(e);
        }
    }

    public static void downloadFileFromURL(URL source, File dest) {
        try (BufferedInputStream in = new BufferedInputStream(source.openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(dest)) {
            byte dataBuffer[] = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            throw RuntimeUtil.getErrorHandler(e).handleError(e);
        }
    }

    public static Path createTempDir() {
        try {
            return Files.createTempDirectory("motion");
        } catch (IOException e) {
            throw RuntimeUtil.getErrorHandler(e).handleError(e);
        }
    }

    public static String formatStruct(Map<?, ?> map) {
        Optional<Function<Object,String>> formatter = RuntimeUtil.getObjectPrinter();
        return formatter.get().apply(map);
    }
}