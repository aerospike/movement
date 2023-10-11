package com.aerospike.movement.performance.report;


import java.util.List;
import java.util.Map;
import java.util.Optional;

//report will encode a csv output of the report
public interface Report {

    public static class Columns {
        public static final String DATE = "date";
        public static final String TIME = "time";

    }

    boolean successful();

    Optional<Map<String, List<String>>> resultData();

}
