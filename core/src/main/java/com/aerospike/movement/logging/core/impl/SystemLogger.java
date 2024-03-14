/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.logging.core.impl;


import com.aerospike.movement.logging.core.Level;
import com.aerospike.movement.logging.core.Logger;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;

import java.util.*;
import java.util.stream.Collectors;

public class SystemLogger implements Logger {
    private final Optional<Object> context;
    private final boolean infoEnabled;
    private final boolean debugEnabled;
    private final boolean warnEnabled;

    public SystemLogger(final Object context) {
        this.context = Optional.ofNullable(context);
        debugEnabled = LocalParallelStreamRuntime.logLevel.equals(Level.DEBUG);
        infoEnabled = LocalParallelStreamRuntime.logLevel.equals(Level.INFO) || LocalParallelStreamRuntime.logLevel.equals(Level.DEBUG);
        warnEnabled = LocalParallelStreamRuntime.logLevel.equals(Level.WARN) || LocalParallelStreamRuntime.logLevel.equals(Level.INFO) || LocalParallelStreamRuntime.logLevel.equals(Level.DEBUG);
    }


    private static String format(final Level level, final Object message, Optional<Object> loggerContext, Object... messageContext) {
        final String messageString;
        if (Map.class.isAssignableFrom(message.getClass()))
            messageString = formatMap((Map) message);
        else if (List.class.isAssignableFrom(message.getClass()))
            messageString = formatList((List) message);
        else
            messageString = message.toString();
        return String.format("[%s][%s] %s", level.name(), loggerContext.orElse("-"), String.format(messageString, messageContext));
    }

    private static String formatList(List message) {
        return "List: [ " +
                String.join("\n  ", (List<String>) message.stream().map(valueObj -> {
                    final String valueStr;
                    if (Map.class.isAssignableFrom(valueObj.getClass()))
                        valueStr = "\n\t\t" + formatMap((Map) valueObj);
                    else if (List.class.isAssignableFrom(valueObj.getClass()))
                        valueStr = "\n\t\t" + formatList((List) valueObj);
                    else
                        valueStr = valueObj.toString();
                    return valueStr;
                }).collect(Collectors.toList())) +
                "\n]";
    }

    private static String formatMap(Map message) {
        List<String> mapEntries = (List<String>) message
                .entrySet()
                .stream()
                .map((entry) -> {
                    final Object valueObj = ((Map.Entry) entry).getValue();
                    final String valueStr;
                    if (Map.class.isAssignableFrom(valueObj.getClass()))
                        valueStr = "\n\t\t" + formatMap((Map) valueObj);
                    else if (List.class.isAssignableFrom(valueObj.getClass()))
                        valueStr = "\n\t\t" + formatList((List) valueObj);
                    else
                        valueStr = valueObj.toString();
                    return ((String) ((Map.Entry) entry).getKey() + ": " + valueStr);
                })
                .collect(Collectors.toList());
        return "Map: " +
                String.join("\t", mapEntries) +
                "\n";
    }

    private void log(Level level, final String message, Object... context) {
        if (context.length > 0) {
            Arrays.stream(context).filter(it -> Throwable.class.isAssignableFrom(it.getClass())).findFirst()
                    .ifPresent(it -> ((Throwable) it).printStackTrace());
        }
        System.out.println(format(level, message, this.context, context));
    }

    @Override
    public void info(final Object message, Object... context) {
        if (infoEnabled)
            System.out.println(format(Level.INFO, message, this.context, context));
    }

    @Override
    public void error(final Object message, Object... context) {
        Arrays.stream(context).filter(it -> Throwable.class.isAssignableFrom(it.getClass())).findFirst()
                .ifPresent(it -> ((Throwable) it).printStackTrace());
        System.err.println(format(Level.ERROR, message, this.context, context));
    }

    @Override
    public void debug(final Object message, Object... context) {
        if (debugEnabled)
            System.out.println(format(Level.DEBUG, message, this.context, context));
    }

    @Override
    public void warn(final Object message, Object... context) {
        if (warnEnabled)
            System.out.println(format(Level.WARN, message, this.context, context));
    }
}
