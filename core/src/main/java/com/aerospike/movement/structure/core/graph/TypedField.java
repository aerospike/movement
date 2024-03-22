package com.aerospike.movement.structure.core.graph;

public class TypedField implements Comparable<TypedField> {
    public final boolean isList;
    public final Class type;
    public final String name;

    public TypedField(final String name, boolean isList, Class type) {
        this.name = name;
        this.isList = isList;
        this.type = type;
    }

    @Override
    public int compareTo(TypedField o) {
        return name.compareTo(o.name);
    }

}