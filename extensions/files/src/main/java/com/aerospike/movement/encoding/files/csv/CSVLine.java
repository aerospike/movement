/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.files.csv;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class CSVLine {
    enum CSVField {
        EMPTY
    }
    private final List<Object> line;
    private final List<String> header;


    public CSVLine(String line, String headerLine) {
        final List<String> header = parseHeader(headerLine);
        this.line = parseLine(header, line);
        this.header = header;
    }

    public Object getEntry(final String key) {
        try {
            return line.get(header.indexOf(key));
        } catch (IndexOutOfBoundsException e) {
//            System.out.println(e);
            throw new RuntimeException("Key not found: " + key);
        }
    }

    public List<String> propertyNames() {
        return header.stream().filter(k -> !k.startsWith("~")).collect(Collectors.toList());
    }

    private static List<String> parseHeader(final String header) {
        final StringTokenizer st = new StringTokenizer(header, ",");
        final List<String> keys = new ArrayList<>();
        while (st.hasMoreTokens()) {
            keys.add(st.nextToken());
        }
        return keys;
    }

    private List<Object> parseLine(List<String> header, String line) {
        StringTokenizer st = new StringTokenizer(line, ",", true);
        List<Object> results = new ArrayList<>();
        for (long i = 0; i < header.size(); i++) {
            String token = st.nextToken();
            if (token.equals(",")) {
                // empty field
                results.add(CSVField.EMPTY);
            } else {
                results.add(token);
                if (st.hasMoreTokens() && !Objects.equals(st.nextToken(), ",")) {
                    throw new RuntimeException("Expected comma after field");
                }
            }
        }
        return results;
    }


}
