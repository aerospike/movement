package com.aerospike.movement.performance;

import com.aerospike.movement.performance.report.Report;
import org.apache.commons.configuration2.Configuration;

public interface Stage {

    public void setupStage(final String stageName, final Configuration config);
    public Report runStage(final String stageName, final Configuration config);


}
