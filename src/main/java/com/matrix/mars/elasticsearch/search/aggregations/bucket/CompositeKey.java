package com.matrix.mars.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;

/**
 * A key that is composed of multiple {@link Comparable} values.
 */
public class CompositeKey implements Writeable {
    @SuppressWarnings("rawtypes")
    private final Comparable[] values;

    @SuppressWarnings("rawtypes")
    CompositeKey(Comparable... values) {
        this.values = values;
    }

    @SuppressWarnings("rawtypes")
    CompositeKey(StreamInput in) throws IOException {
        values = new Comparable[in.readVInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = (Comparable) in.readGenericValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.length);
        for (int i = 0; i < values.length; i++) {
            out.writeGenericValue(values[i]);
        }
    }

    @SuppressWarnings("rawtypes")
    Comparable[] values() {
        return values;
    }

    int size() {
        return values.length;
    }

    @SuppressWarnings("rawtypes")
    Comparable get(int pos) {
        assert pos < values.length;
        return values[pos];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "CompositeKey{" +
                "values=" + Arrays.toString(values) +
                '}';
    }
}
