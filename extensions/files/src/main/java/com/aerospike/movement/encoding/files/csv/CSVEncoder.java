/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.structure.core.graph.TypedField;
import com.aerospike.movement.util.core.error.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class CSVEncoder extends Loadable implements Encoder<String> {
    private static final String LIST_DELIMITER = ";";

    protected CSVEncoder(final ConfigurationBase configurationMeta, Configuration config) {
        super(configurationMeta, config);
    }

    public static String toCsvLine(final List<String> fields) {
        Optional<String> x = fields.stream().reduce((a, b) -> a + "," + b);
        return x.get();
    }

    public static String toCSVHeader(final List<TypedField> fields) {
        final List<String> typedFields = new ArrayList<>();
        fields.forEach(typedField -> {
            if (typedField.type.equals(String.class) && !typedField.isList) {
                typedFields.add(typedField.name);
                return;
            }
            final StringBuilder sb = new StringBuilder();
            sb.append(typedField.name);
            sb.append(":");
            sb.append(toCSVType(typedField.type));
            if (typedField.isList)
                sb.append("[]");
            typedFields.add(sb.toString());

        });
        Optional<String> x = typedFields.stream().reduce((a, b) -> a + "," + b);
        return x.get();
    }

    public static String toCSVType(Class jvmType) {
        if (jvmType.equals(String.class))
            return "string";
        if (jvmType.equals(Integer.class))
            return "int";
        if (jvmType.equals(Long.class))
            return "long";
        if (jvmType.equals(Boolean.class))
            return "bool";
        return "string";
    }

    public static TypedField fromCSVType(String csvField) {
        if (!csvField.contains(":"))
            return new TypedField(csvField, false, String.class);
        String[] components = csvField.split(":");
        final String nameComponent = components[0];
        final String typeComponent = components[1];
        final boolean list = typeComponent.endsWith("[]");
        Class type;
        switch (typeComponent.toLowerCase()) {
            case "string":
                type = String.class;
                break;
            case "int":
                type = Integer.class;
                break;
            case "long":
                type = Long.class;
                break;
            case "boolean":
                type = Boolean.class;
                break;
            case "bool":
                type = Boolean.class;
                break;
            default:
                type = String.class;
                break;
        }
        return new TypedField(nameComponent, list, type);
    }

    public static Object decodeEntry(final TypedField type, final Object maybeEntry) {
        if(maybeEntry.equals(CSVLine.CSVField.EMPTY))
            return maybeEntry;
        final String entry = (String) maybeEntry;
        if (type.isList)
            return Arrays.stream(entry.split(LIST_DELIMITER)).map(subEntry ->
                            decodeEntry(new TypedField(type.name, false, type.type), subEntry))
                    .collect(Collectors.toList());
        switch (toCSVType(type.type)) {
            case "string":
                return entry;
            case "int":
                return Integer.parseInt(entry);
            case "long":
                return Long.parseLong(entry);
            case "boolean":
                return Boolean.parseBoolean(entry);
            case "bool":
                return Boolean.parseBoolean(entry);
            default:
                return entry;
        }

    }


    @Override
    public Optional<String> encode(final Emitable item) {
        if (Optional.class.isAssignableFrom(item.getClass()))
            throw new RuntimeException("optional");
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return Optional.of(CSVEncoder.toCsvLine(toCsvFields((EmittedEdge) item)));
        }
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return Optional.of(CSVEncoder.toCsvLine(toCsvFields((EmittedVertex) item)));
        }
        throw ErrorUtil.runtimeException("Cannot encode %s", item.getClass().getName());
    }

    protected abstract List<String> toCsvFields(final Emitable item);
}
