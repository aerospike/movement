package com.aerospike.movement.plugin;

import java.util.List;
import java.util.Map;

public interface PluginInterface {
    class Methods {
        public static String API = "api";
        public static String PLUG_INTO = "plugInto";
    }

    Map<String, List<String>> api();

    void plugInto(Object system);
}
