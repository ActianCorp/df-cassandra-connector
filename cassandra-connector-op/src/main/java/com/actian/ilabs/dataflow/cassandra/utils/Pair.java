package com.actian.ilabs.dataflow.cassandra.utils;

public class Pair<X,Y> {

    public final X x;

    public final Y y;

    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public static <S,T> Pair<S,T> of(S s, T t){
        return new Pair<S,T>(s, t);
    }
}
