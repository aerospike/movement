package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.emitter.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.List;
import java.util.Optional;

public abstract class CSVEncoder extends Loadable implements Encoder<String> {
    protected CSVEncoder(final ConfigurationBase configurationMeta, Configuration config) {
        super(configurationMeta, config);
    }

    public static String toCsvLine(final List<String> fields) {
        Optional<String> x = fields.stream().reduce((a, b) -> a + "," + b);
        return x.get();
    }


    @Override
    public String encode(final Emitable item) {
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return CSVEncoder.toCsvLine(toCsvFields((EmittedEdge) item));
        }
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return CSVEncoder.toCsvLine(toCsvFields((EmittedVertex) item));
        }
        throw ErrorUtil.runtimeException("Cannot encode %s", item.getClass().getName());
    }

    protected abstract List<String> toCsvFields(final Emitable item);
}
