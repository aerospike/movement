/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.structure.core.graph.TypedField;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import static com.aerospike.movement.encoding.files.csv.CSVEncoder.fromCSVType;

public class CSVLine {
    public enum CSVField {
        EMPTY
    }

    private final List<Object> line;
    private final List<TypedField> header;



    public CSVLine(String line, String headerLine) {
        final List<TypedField> header = parseHeader(headerLine);
        this.line = parseLine(header, line);
        this.header = header;
    }

    public Object getEntry(final String key) {
        try {
            return CSVEncoder.decodeEntry(getType(key),line.get(getHeaderIndex(key)));
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("Key not found: " + key);
        }
    }
    public TypedField getType(final String key) {
        try {
            return header.get(getHeaderIndex(key));
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("Key not found: " + key);
        }
    }

    private int getHeaderIndex(String key) {
       return header.stream().map(tf -> tf.name).collect(Collectors.toList()).indexOf(key);
    }

    public List<String> propertyNames() {
        return header.stream().filter(k -> !k.name.startsWith("~")).map(it->it.name).collect(Collectors.toList());
    }

    private static List<TypedField> parseHeader(final String header) {

        final StringTokenizer st = new StringTokenizer(header, ",");
        final List<TypedField> keys = new ArrayList<>();
        while (st.hasMoreTokens()) {
            keys.add(fromCSVType(st.nextToken()));
        }
        return keys;
    }

    private List<Object> parseLine(List<TypedField> header, String line) {
        String[] split = line.split(",", -1);
        List<Object> results = new ArrayList<>();
        for (int i = 0; i < header.size(); i++) {
            String token = split[i];
            if (token.isEmpty()) {
                results.add(CSVField.EMPTY);
            } else {
                results.add(token);
            }
        }
        return results;
    }

    @Override
    public String toString() {
        return String.join(",", line.stream().map(it -> it.equals(CSVField.EMPTY) ? "" : it.toString()).collect(Collectors.toList()));
    }


}
