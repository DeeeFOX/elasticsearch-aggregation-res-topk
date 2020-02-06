package com.matrix.mars.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Iterator;
import java.util.AbstractMap;
import java.util.Set;
import java.util.AbstractSet;

/**
 * An internal implementation of {@link InternalMultiBucketAggregation} which extends {@link InternalMultiBucketAggregation}.
 * Mainly, returns the builder and makes the reduce of buckets.
 */
public class InternalBucketTopk extends InternalMultiBucketAggregation<
        InternalBucketTopk, InternalBucketTopk.InternalBucket> implements CompositeAggregation {

    private final int size;
    private final List<InternalBucketTopk.InternalBucket> buckets;
    private final CompositeKey afterKey;
    private final int[] reverseMuls;
    private final List<String> sourceNames;
    private final List<DocValueFormat> formats;

    InternalBucketTopk(String name, int size, List<String> sourceNames, List<DocValueFormat> formats,
                       List<InternalBucketTopk.InternalBucket> buckets, CompositeKey afterKey, int[] reverseMuls,
                       List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sourceNames = sourceNames;
        this.formats = formats;
        this.buckets = buckets;
        this.afterKey = afterKey;
        this.size = size;
        this.reverseMuls = reverseMuls;
    }

    public InternalBucketTopk(StreamInput in) throws IOException {
        super(in);
        this.size = in.readVInt();
        this.sourceNames = in.readStringList();
        this.formats = new ArrayList<>(sourceNames.size());
        for (int i = 0; i < sourceNames.size(); i++) {
            if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
                formats.add(in.readNamedWriteable(DocValueFormat.class));
            } else {
                formats.add(DocValueFormat.RAW);
            }
        }
        this.reverseMuls = in.readIntArray();
        this.buckets = in.readList((input) -> new InternalBucketTopk.InternalBucket(input, sourceNames, formats, reverseMuls));
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            this.afterKey = in.readBoolean() ? new CompositeKey(in) : null;
        } else {
            this.afterKey = buckets.size() > 0 ? buckets.get(buckets.size() - 1).key : null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        out.writeStringCollection(sourceNames);
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            for (DocValueFormat format : formats) {
                out.writeNamedWriteable(format);
            }
        }
        out.writeIntArray(reverseMuls);
        out.writeList(buckets);
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeBoolean(afterKey != null);
            if (afterKey != null) {
                afterKey.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return CompositeAggregation.toXContentFragment(this, builder, params);
    }

    @Override
    public String getWriteableName() {
        return CompositeAggregationBuilder.NAME;
    }

    @Override
    public InternalBucketTopk create(List<InternalBucketTopk.InternalBucket> newBuckets) {
        /**
         * This is used by pipeline aggregations to filter/remove buckets so we
         * keep the <code>afterKey</code> of the original aggregation in order
         * to be able to retrieve the next page even if all buckets have been filtered.
         */
        return new InternalBucketTopk(name, size, sourceNames, formats, newBuckets, afterKey,
                reverseMuls, pipelineAggregators(), getMetaData());
    }

    @Override
    public InternalBucketTopk.InternalBucket createBucket(InternalAggregations aggregations, InternalBucketTopk.InternalBucket prototype) {
        return new InternalBucketTopk.InternalBucket(prototype.sourceNames, prototype.formats, prototype.key, prototype.reverseMuls,
                prototype.docCount, aggregations);
    }

    public int getSize() {
        return size;
    }

    @Override
    public List<InternalBucketTopk.InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    public Map<String, Object> afterKey() {
        if (afterKey != null) {
            return new InternalBucketTopk.ArrayMap(sourceNames, formats, afterKey.values());
        }
        return null;
    }

    // Visible for tests
    int[] getReverseMuls() {
        return reverseMuls;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        PriorityQueue<InternalBucketTopk.BucketIterator> pq = new PriorityQueue<>(aggregations.size());
        for (InternalAggregation agg : aggregations) {
            InternalBucketTopk sortedAgg = (InternalBucketTopk) agg;
            InternalBucketTopk.BucketIterator it = new InternalBucketTopk.BucketIterator(sortedAgg.buckets);
            if (it.next() != null) {
                pq.add(it);
            }
        }
        InternalBucketTopk.InternalBucket lastBucket = null;
        List<InternalBucketTopk.InternalBucket> buckets = new ArrayList<>();
        List<InternalBucketTopk.InternalBucket> result = new ArrayList<>();
        while (pq.size() > 0) {
            InternalBucketTopk.BucketIterator bucketIt = pq.poll();
            if (lastBucket != null && bucketIt.current.compareKey(lastBucket) != 0) {
                InternalBucketTopk.InternalBucket reduceBucket = reduceBucket(buckets, reduceContext);
                buckets.clear();
                reduceContext.consumeBucketsAndMaybeBreak(1);
                result.add(reduceBucket);
                if (result.size() >= size) {
                    break;
                }
            }
            lastBucket = bucketIt.current;
            buckets.add(bucketIt.current);
            if (bucketIt.next() != null) {
                pq.add(bucketIt);
            }
        }
        if (buckets.size() > 0) {
            InternalBucketTopk.InternalBucket reduceBucket = reduceBucket(buckets, reduceContext);
            reduceContext.consumeBucketsAndMaybeBreak(1);
            result.add(reduceBucket);
        }
        final CompositeKey lastKey = result.size() > 0 ? result.get(result.size() - 1).getRawKey() : null;
        return new InternalBucketTopk(name, size, sourceNames, formats, result, lastKey, reverseMuls, pipelineAggregators(), metaData);
    }

    @Override
    protected InternalBucketTopk.InternalBucket reduceBucket(List<InternalBucketTopk.InternalBucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (InternalBucketTopk.InternalBucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregations.add(bucket.aggregations);
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return new InternalBucketTopk.InternalBucket(sourceNames, formats, buckets.get(0).key, reverseMuls, docCount, aggs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalBucketTopk that = (InternalBucketTopk) obj;
        return Objects.equals(size, that.size) &&
                Objects.equals(buckets, that.buckets) &&
                Objects.equals(afterKey, that.afterKey) &&
                Arrays.equals(reverseMuls, that.reverseMuls);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), size, buckets, afterKey, Arrays.hashCode(reverseMuls));
    }

    private static class BucketIterator implements Comparable<InternalBucketTopk.BucketIterator> {
        final Iterator<InternalBucketTopk.InternalBucket> it;
        InternalBucketTopk.InternalBucket current;

        private BucketIterator(List<InternalBucketTopk.InternalBucket> buckets) {
            this.it = buckets.iterator();
        }

        @Override
        public int compareTo(InternalBucketTopk.BucketIterator other) {
            return current.compareKey(other.current);
        }

        InternalBucketTopk.InternalBucket next() {
            return current = it.hasNext() ? it.next() : null;
        }
    }

    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket
            implements CompositeAggregation.Bucket, KeyComparable<InternalBucket> {

        private final CompositeKey key;
        private final long docCount;
        private final InternalAggregations aggregations;
        private final transient int[] reverseMuls;
        private final transient List<String> sourceNames;
        private final transient List<DocValueFormat> formats;


        InternalBucket(List<String> sourceNames, List<DocValueFormat> formats, CompositeKey key, int[] reverseMuls, long docCount,
                       InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.reverseMuls = reverseMuls;
            this.sourceNames = sourceNames;
            this.formats = formats;
        }

        InternalBucket(StreamInput in, List<String> sourceNames, List<DocValueFormat> formats, int[] reverseMuls) throws IOException {
            this.key = new CompositeKey(in);
            this.docCount = in.readVLong();
            this.aggregations = new InternalAggregations(in);
            this.reverseMuls = reverseMuls;
            this.sourceNames = sourceNames;
            this.formats = formats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            key.writeTo(out);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, key, aggregations);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            InternalBucketTopk.InternalBucket that = (InternalBucketTopk.InternalBucket) obj;
            return Objects.equals(docCount, that.docCount)
                    && Objects.equals(key, that.key)
                    && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public Map<String, Object> getKey() {
            // returns the formatted key in a map
            return new InternalBucketTopk.ArrayMap(sourceNames, formats, key.values());
        }

        // get the raw key (without formatting to preserve the natural order).
        // visible for testing
        CompositeKey getRawKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            for (int i = 0; i < key.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(sourceNames.get(i));
                builder.append('=');
                builder.append(formatObject(key.get(i), formats.get(i)));
            }
            builder.append('}');
            return builder.toString();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public int compareKey(InternalBucketTopk.InternalBucket other) {
            for (int i = 0; i < key.size(); i++) {
                if (key.get(i) == null) {
                    if (other.key.get(i) == null) {
                        continue;
                    }
                    return -1 * reverseMuls[i];
                } else if (other.key.get(i) == null) {
                    return reverseMuls[i];
                }
                assert key.get(i).getClass() == other.key.get(i).getClass();
                @SuppressWarnings("unchecked")
                int cmp = key.get(i).compareTo(other.key.get(i)) * reverseMuls[i];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /**
             * See {@link CompositeAggregation#bucketToXContent}
             */
            throw new UnsupportedOperationException("not implemented");
        }
    }

    /**
     * Format <code>obj</code> using the provided {@link DocValueFormat}.
     * If the format is equals to {@link DocValueFormat#RAW}, the object is returned as is
     * for numbers and a string for {@link BytesRef}s.
     */
    static Object formatObject(Object obj, DocValueFormat format) {
        if (obj == null) {
            return null;
        }
        if (obj.getClass() == BytesRef.class) {
            BytesRef value = (BytesRef) obj;
            if (format == DocValueFormat.RAW) {
                return value.utf8ToString();
            } else {
                return format.format(value);
            }
        } else if (obj.getClass() == Long.class) {
            long value = (long) obj;
            if (format == DocValueFormat.RAW) {
                return value;
            } else {
                return format.format(value);
            }
        } else if (obj.getClass() == Double.class) {
            double value = (double) obj;
            if (format == DocValueFormat.RAW) {
                return value;
            } else {
                return format.format(value);
            }
        }
        return obj;
    }

    @SuppressWarnings("rawtypes")
    static class ArrayMap extends AbstractMap<String, Object> implements Comparable<InternalBucketTopk.ArrayMap> {
        final List<String> keys;
        final Comparable[] values;
        final List<DocValueFormat> formats;

        ArrayMap(List<String> keys, List<DocValueFormat> formats, Comparable[] values) {
            assert keys.size() == values.length && keys.size() == formats.size();
            this.keys = keys;
            this.formats = formats;
            this.values = values;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Object get(Object key) {
            for (int i = 0; i < keys.size(); i++) {
                if (key.equals(keys.get(i))) {
                    return formatObject(values[i], formats.get(i));
                }
            }
            return null;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<Entry<String, Object>>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<Entry<String, Object>>() {
                        int pos = 0;

                        @Override
                        public boolean hasNext() {
                            return pos < values.length;
                        }

                        @Override
                        public Entry<String, Object> next() {
                            SimpleEntry<String, Object> entry =
                                    new SimpleEntry<>(keys.get(pos), formatObject(values[pos], formats.get(pos)));
                            ++pos;
                            return entry;
                        }
                    };
                }

                @Override
                public int size() {
                    return keys.size();
                }
            };
        }

        @Override
        public int compareTo(InternalBucketTopk.ArrayMap that) {
            if (that == this) {
                return 0;
            }

            int idx = 0;
            int max = Math.min(this.keys.size(), that.keys.size());
            while (idx < max) {
                int compare = compareNullables(keys.get(idx), that.keys.get(idx));
                if (compare == 0) {
                    compare = compareNullables(values[idx], that.values[idx]);
                }
                if (compare != 0) {
                    return compare;
                }
                idx++;
            }
            if (idx < keys.size()) {
                return 1;
            }
            if (idx < that.keys.size()) {
                return -1;
            }
            return 0;
        }
    }

    @SuppressWarnings("rawtypes")
    private static int compareNullables(Comparable a, Comparable b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        @SuppressWarnings("unchecked")
        int ret = a.compareTo(b);
        return ret;
    }
}
