package com.aerospike.graph.move;

import org.apache.tinkerpop.gremlin.structure.*;

import java.lang.reflect.Method;
import java.util.*;

public class VirtualPlugin {

    public interface API {
        default String[] getAPIMethodNames() {
            return Arrays
                    .stream(API.class.getMethods())
                    .map(Method::getName)
                    .filter(it -> !it.equals("getAPIMethodNames"))
                    .toArray(String[]::new);
        }


        //Returns id
        String run();

        boolean isRunning();

        void stop();

        Map<String, Object> getStatus();

    }

    public static Object open(final Graph graph) {
        return Plugin.INSTANCE;
    }


    private static class Plugin implements API {
        private static final Plugin INSTANCE = new Plugin();

    }
}
