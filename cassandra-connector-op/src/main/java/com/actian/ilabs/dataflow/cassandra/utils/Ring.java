package com.actian.ilabs.dataflow.cassandra.utils;

import java.util.Arrays;

public class Ring<T> {

    private final T[] values;
    private int cursor;
    
    public Ring(T... values) {
        this.values = Arrays.copyOf(values, values.length);
        cursor = 0;
    }
    
    public T next() {
        if (cursor >= values.length) {
            cursor = 0;
        }
        return values[cursor++];
    }
}