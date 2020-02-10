package com.matrix.mars.elasticsearch.search.aggregation.bucket.topk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Iterator;
import java.util.AbstractMap;
import java.util.Set;
import java.util.AbstractSet;

/**
 * Inspired by InternalComposite
 */
public class InternalTopk extends InternalMultiBucketAggregation<InternalTopk, InternalTopk.InternalBucket> implements TopkAggregation {

    private final int from;
    private final int size;
    private final List<InternalBucket> buckets;
    private final int[] sourceReverseMuls;
    private final List<String> sourceNames;
    private final List<DocValueFormat> sourceFormats;

    public InternalTopk(String name, int from, int size, List<String> sourceNames, List<DocValueFormat> sourceFormats,
                        List<InternalBucket> buckets, int[] sourceReverseMuls,
                        List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sourceNames = sourceNames;
        this.sourceFormats = sourceFormats;
        this.buckets = buckets;
        this.size = size;
        this.from = from;
        this.sourceReverseMuls = sourceReverseMuls;
    }

    protected InternalTopk(StreamInput in) throws IOException {
        super(in);
        this.from = in.readVInt();
        this.size = in.readVInt();
        this.sourceNames = in.readStringList();
        this.sourceFormats = new ArrayList<>(sourceNames.size());
        for (int i = 0; i < sourceNames.size(); i++) {
            if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
                sourceFormats.add(in.readNamedWriteable(DocValueFormat.class));
            } else {
                sourceFormats.add(DocValueFormat.RAW);
            }
        }
        this.sourceReverseMuls = in.readIntArray();
        this.buckets = in.readList((input) -> new InternalBucket(input, sourceNames, sourceFormats, sourceReverseMuls));
    }

    @Override
    public InternalTopk create(List<InternalBucket> buckets) {
        /**
         * This is used by pipeline aggregations to filter/remove buckets so we
         * keep the <code>afterKey</code> of the original aggregation in order
         * to be able to retrieve the next page even if all buckets have been filtered.
         */
        return new InternalTopk(name, from, size, sourceNames, sourceFormats, buckets,
                sourceReverseMuls, pipelineAggregators(), getMetaData());
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(
                prototype.sourceNames, prototype.sourceFormats, prototype.key, prototype.sourceReverseMuls,
                prototype.docCount, aggregations);
    }

    @Override
    protected InternalBucket reduceBucket(List<InternalBucket> buckets, ReduceContext context) {
        // TODO
        // Check if just simple reduce or topk reduce
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (InternalBucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregations.add(bucket.aggregations);
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return new InternalBucket(sourceNames, sourceFormats, buckets.get(0).key, sourceReverseMuls, docCount, aggs);
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeStringCollection(sourceNames);
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            for (DocValueFormat format : sourceFormats) {
                out.writeNamedWriteable(format);
            }
        }
        out.writeIntArray(sourceReverseMuls);
        out.writeList(buckets);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // TODO
        // Should be a topk k topk reduce not simple reduce
        PriorityQueue<BucketIterator> pq = new PriorityQueue<>(aggregations.size());
        for (InternalAggregation agg : aggregations) {
            InternalTopk sortedAgg = (InternalTopk) agg;
            BucketIterator it = new BucketIterator(sortedAgg.buckets);
            if (it.next() != null) {
                pq.add(it);
            }
        }
        InternalBucket lastBucket = null;
        List<InternalBucket> buckets = new ArrayList<>();
        List<InternalBucket> result = new ArrayList<>();
        while (pq.size() > 0) {
            BucketIterator bucketIt = pq.poll();
            if (lastBucket != null && bucketIt.current.compareKey(lastBucket) != 0) {
                InternalBucket reduceBucket = reduceBucket(buckets, reduceContext);
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
            InternalBucket reduceBucket = reduceBucket(buckets, reduceContext);
            reduceContext.consumeBucketsAndMaybeBreak(1);
            result.add(reduceBucket);
        }
        return new InternalTopk(name, from, size, sourceNames, sourceFormats, result, sourceReverseMuls, pipelineAggregators(), metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return TopkAggregation.toXContentFragment(this, builder, params);
    }

    @Override
    public String getWriteableName() {
        return TopkAggregationBuilder.NAME;
    }

    private static class BucketIterator implements Comparable<InternalTopk.BucketIterator> {
        final Iterator<InternalBucket> it;
        InternalBucket current;

        private BucketIterator(List<InternalBucket> buckets) {
            this.it = buckets.iterator();
        }

        @Override
        public int compareTo(BucketIterator other) {
            return current.compareKey(other.current);
        }

        InternalBucket next() {
            return current = it.hasNext() ? it.next() : null;
        }
    }

    /**
     * Inspired by InternalComposite.InternalBucket
     */
    public static class InternalBucket
            extends InternalMultiBucketAggregation.InternalBucket
            implements TopkAggregation.Bucket, KeyComparable<InternalBucket> {

        private final CompositeKey key;
        private final long docCount;
        private final InternalAggregations aggregations;
        private final transient int[] sourceReverseMuls;
        private final transient List<String> sourceNames;
        private final transient List<DocValueFormat> sourceFormats;

        InternalBucket(
                List<String> sourceNames, List<DocValueFormat> sourceFormats, CompositeKey key,
                int[] sourceReverseMuls, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.sourceReverseMuls = sourceReverseMuls;
            this.sourceNames = sourceNames;
            this.sourceFormats = sourceFormats;
        }

        InternalBucket(
                StreamInput in, List<String> sourceNames,
                List<DocValueFormat> sourceFormats, int[] sourceReverseMuls) throws IOException {
            this.key = new CompositeKey(in);
            this.docCount = in.readVLong();
            this.aggregations = new InternalAggregations(in);
            this.sourceReverseMuls = sourceReverseMuls;
            this.sourceNames = sourceNames;
            this.sourceFormats = sourceFormats;
        }

        @Override
        public Map<String, Object> getKey() {
            // returns the formatted key in a map
            return new InternalTopk.ArrayMap(sourceNames, sourceFormats, key.values());
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
                builder.append(formatObject(key.get(i), sourceFormats.get(i)));
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

        /**
         * This allows them to be "thrown across the wire" using Elasticsearch's internal protocol
         * see {@link Writeable}
         */
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
            InternalTopk.InternalBucket that = (InternalTopk.InternalBucket) obj;
            return Objects.equals(docCount, that.docCount)
                    && Objects.equals(key, that.key)
                    && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int compareKey(InternalBucket other) {
            for (int i = 0; i < key.size(); i++) {
                if (key.get(i) == null) {
                    if (other.key.get(i) == null) {
                        continue;
                    }
                    return -1 * sourceReverseMuls[i];
                } else if (other.key.get(i) == null) {
                    return sourceReverseMuls[i];
                }
                assert key.get(i).getClass() == other.key.get(i).getClass();
                @SuppressWarnings("unchecked")
                int cmp = key.get(i).compareTo(other.key.get(i)) * sourceReverseMuls[i];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /**
             * See {@link TopkAggregation#bucketToXContent}
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

    /**
     * Inspired by InternalComposite ArrayMap
     * Comparable key sets
     */
    @SuppressWarnings("rawtypes")
    static class ArrayMap extends AbstractMap<String, Object> implements Comparable<InternalTopk.ArrayMap> {
        final List<String> keys;
        final Comparable[] values;
        final List<DocValueFormat> formats;

        @SuppressWarnings("rawtypes")
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
        public int compareTo(InternalTopk.ArrayMap that) {
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
