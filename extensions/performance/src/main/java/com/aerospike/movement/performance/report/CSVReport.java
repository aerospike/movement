package com.aerospike.movement.performance.report;

import java.util.List;
import java.util.Map;
import java.util.Optional;
/*
  A report entry could be an emitable, and encoding it to csv should be done by an Encoder. it should be written to an Output.
 */
public class CSVReport implements Report {

    public static class Columns extends Report.Columns {

    }


    @Override
    public boolean successful() {
        return false;
    }

    @Override
    public Optional<Map<String, List<String>>> resultData() {
        if (!successful()) {
            return Optional.empty();
        }
        return Optional.empty();
    }
}
