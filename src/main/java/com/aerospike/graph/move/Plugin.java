package com.aerospike.graph.move;

import com.aerospike.graph.move.util.ErrorUtil;
import org.apache.tinkerpop.gremlin.structure.*;

import java.lang.reflect.Method;
import java.util.*;

public class Plugin {
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
        return PluginImpl.INSTANCE;
    }


    private static class PluginImpl implements API {
        private static final PluginImpl INSTANCE = new PluginImpl();

        @Override
        public String run() {
            throw ErrorUtil.unimplemented();
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public void stop() {

        }

        @Override
        public Map<String, Object> getStatus() {
            throw ErrorUtil.unimplemented();
        }
    }
}
