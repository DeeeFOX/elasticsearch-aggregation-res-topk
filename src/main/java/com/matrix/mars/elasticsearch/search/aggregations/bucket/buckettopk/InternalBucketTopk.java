package com.matrix.mars.elasticsearch.search.aggregations.bucket.buckettopk;

import com.google.common.collect.Lists;
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
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Arrays;
import java.util.Iterator;
import java.util.AbstractMap;
import java.util.Set;
import java.util.AbstractSet;

/**
 * An internal implementation of {@link BucketTopkAggregation} which extends {@link InternalMultiBucketAggregation}.
 * Mainly, returns the builder and makes the reduce of buckets.
 * Inspired by {@link org.elasticsearch.search.aggregations.bucket.composite.InternalComposite}
 */
public class InternalBucketTopk extends InternalMultiBucketAggregation<
        InternalBucketTopk, InternalBucketTopk.InternalBucket> implements BucketTopkAggregation {

    // Aggregator's parameters
    private final int from;
    private final int size;
    private final String baseKeyName;
    private final FieldSortBuilder sort;

    private final int bucketSize;
    private final List<InternalBucketTopk.InternalBucket> buckets;
    private final int[] keyReverseMuls;
    private final List<String> keyNames;
    private final List<DocValueFormat> keyFormats;


    InternalBucketTopk(
            String name, int bucketSize, List<String> keyNames, List<DocValueFormat> keyFormats,
            List<InternalBucketTopk.InternalBucket> buckets, int[] keyReverseMuls,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
            int from, int size, String baseKeyName, FieldSortBuilder sort
    ) {
        super(name, pipelineAggregators, metaData);
        this.bucketSize = bucketSize;
        this.keyNames = keyNames;
        this.keyFormats = keyFormats;
        this.keyReverseMuls = keyReverseMuls;
        this.buckets = buckets;
        this.from = from;
        this.size = size;
        this.baseKeyName = baseKeyName;
        this.sort = sort;
    }

    public InternalBucketTopk(StreamInput in) throws IOException {
        super(in);
        this.bucketSize = in.readVInt();
        this.keyNames = in.readStringList();
        this.keyFormats = new ArrayList<>(keyNames.size());
        for (int i = 0; i < keyNames.size(); i++) {
            if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
                keyFormats.add(in.readNamedWriteable(DocValueFormat.class));
            } else {
                keyFormats.add(DocValueFormat.RAW);
            }
        }
        this.keyReverseMuls = in.readIntArray();
        this.buckets = in.readList(
                (input) -> new InternalBucketTopk.InternalBucket(
                        input, keyNames, keyFormats, keyReverseMuls));
        this.from = in.readVInt();
        this.size = in.readVInt();
        this.baseKeyName = in.readString();
        this.sort = new FieldSortBuilder(in);

    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(bucketSize);
        out.writeStringCollection(keyNames);
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            for (DocValueFormat format : keyFormats) {
                out.writeNamedWriteable(format);
            }
        }
        out.writeIntArray(keyReverseMuls);
        out.writeList(buckets);
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeString(baseKeyName);
        sort.writeTo(out);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return BucketTopkAggregation.toXContentFragment(this, builder, params);
    }

    @Override
    public String getWriteableName() {
        return BucketTopkPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalBucketTopk create(List<InternalBucketTopk.InternalBucket> newBuckets) {
        /**
         * This is used by pipeline aggregations to filter/remove buckets.
         */
        return new InternalBucketTopk(name, bucketSize, keyNames, keyFormats, newBuckets,
                keyReverseMuls, pipelineAggregators(), getMetaData(), from, size, baseKeyName, sort);
    }

    @Override
    public InternalBucketTopk.InternalBucket createBucket(
            InternalAggregations aggregations, InternalBucketTopk.InternalBucket orgBucket) {
        return new InternalBucketTopk.InternalBucket(
                orgBucket.keyNames, orgBucket.keyFormats, orgBucket.key, orgBucket.keyReverseMuls, orgBucket.docCount, aggregations,
                orgBucket.baseKeyValues, orgBucket.orgTreeNode);
    }

    public int getSize() {
        return bucketSize;
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    // Visible for tests
    int[] getReverseMuls() {
        return keyReverseMuls;
    }

    /**
     * Do reduce work from different shards
     * Like what {@link org.elasticsearch.search.aggregations.bucket.terms.InternalTerms}::doReduce do
     * Handle this aggregation's result (InternalAggregation(s)) from other shards
     * Collect all the buckets and map into different key bucket
     * Then pass to reduceBucket method to reduce every same key bucket into a final bucket result
     * And reduce into one InternalAggregation(here InternalBucketTopk) of its own
     *
     * @param aggregations  InternalBucketTopk(s) list from other shards
     * @param reduceContext ReduceContext
     * @return InternalBucketTopk
     */
    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        TopkForest topkForest = new TopkForest(new String[]{this.baseKeyName}, from, size, sort);
        for (InternalAggregation agg : aggregations) {
            InternalBucketTopk sortedAgg = (InternalBucketTopk) agg;
            InternalBucketTopk.BucketIterator it = new InternalBucketTopk.BucketIterator(sortedAgg.buckets);
            if (it.next() != null) {
                topkForest.addOthShardsBucket(it);
            }
        }
        List<InternalBucketTopk.InternalBucket> result = new ArrayList<>();
        for (ArrayMap key: topkForest.sortedKeySet()) {
            InternalBucketTopk.InternalBucket reduceBucket = reduceBucket(
                    topkForest.get(key).getNodesBuckets(from, size), reduceContext);
            reduceContext.consumeBucketsAndMaybeBreak(1);
            result.add(reduceBucket);
        }
        return new InternalBucketTopk(
                name, bucketSize, keyNames, keyFormats, result, keyReverseMuls, pipelineAggregators(), metaData,
                from, size, baseKeyName, sort
        );
    }

    /**
     * Reduce a list of same-keyed buckets (from multiple shards) to a single bucket. This
     * requires all buckets to have the same key.
     * Strongly Inspired by {@link org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram}::reduceBucket
     *
     * @param buckets same key buckets to reduce
     * @param context reduce context
     * @return reduced bucket
     */
    @Override
    protected InternalBucketTopk.InternalBucket reduceBucket(List<InternalBucketTopk.InternalBucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (InternalBucket bucket : buckets) {
            aggregationsList.add(bucket.aggregations);
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return createBucket(aggs, buckets.get(0));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalBucketTopk that = (InternalBucketTopk) obj;
        return Objects.equals(bucketSize, that.bucketSize) &&
                Objects.equals(buckets, that.buckets) &&
                Arrays.equals(keyReverseMuls, that.keyReverseMuls);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketSize, buckets, Arrays.hashCode(keyReverseMuls));
    }

    public static class BucketIterator implements Comparable<InternalBucketTopk.BucketIterator> {
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

    /**
     * InternalBucket of the
     * Inspired by the {@link org.elasticsearch.search.aggregations.bucket.composite.InternalComposite.InternalBucket}
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket
            implements BucketTopkAggregation.Bucket, KeyComparable<InternalBucket>, Writeable {

        private final CompositeKey key;
        private final long docCount;
        private final InternalAggregations aggregations;
        private final transient int[] keyReverseMuls;
        private final transient List<String> keyNames;
        private final transient List<DocValueFormat> keyFormats;

        private final transient ArrayMap baseKeyValues;
        private final TopkForest.TopkTree.TopkTreeNode orgTreeNode;


        InternalBucket(
                List<String> keyNames, List<DocValueFormat> keyFormats, CompositeKey key, int[] keyReverseMuls, long docCount,
                InternalAggregations aggregations, ArrayMap baseKeyValues, TopkForest.TopkTree.TopkTreeNode treeNode) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.keyReverseMuls = keyReverseMuls;
            this.keyNames = keyNames;
            this.keyFormats = keyFormats;
            this.baseKeyValues = baseKeyValues;
            this.orgTreeNode = treeNode;
        }

        InternalBucket(
                StreamInput in, List<String> keyNames, List<DocValueFormat> keyFormats, int[] keyReverseMuls) throws IOException {
            this.key = new CompositeKey(in);
            this.docCount = in.readVLong();
            this.aggregations = new InternalAggregations(in);
            // user defined attributes
            this.keyReverseMuls = keyReverseMuls;
            this.keyNames = keyNames;
            this.keyFormats = keyFormats;
            this.baseKeyValues = new ArrayMap(in);
            this.orgTreeNode = new TopkForest.TopkTree.TopkTreeNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            key.writeTo(out);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
            baseKeyValues.writeTo(out);
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

        public ArrayMap getBaseKeyValues() {
            return this.baseKeyValues;
        }

        @Override
        public Map<String, Object> getKey() {
            // returns the formatted key in a map
            return new InternalBucketTopk.ArrayMap(keyNames, keyFormats, key.values());
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
                builder.append(keyNames.get(i));
                builder.append('=');
                builder.append(formatObject(key.get(i), keyFormats.get(i)));
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
                    return -1 * keyReverseMuls[i];
                } else if (other.key.get(i) == null) {
                    return keyReverseMuls[i];
                }
                assert key.get(i).getClass() == other.key.get(i).getClass();
                @SuppressWarnings("unchecked")
                int cmp = key.get(i).compareTo(other.key.get(i)) * keyReverseMuls[i];
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

        public TopkForest.TopkTree.TopkTreeNode getOrgTokTreeNode() {
            return this.orgTreeNode;
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
    static class ArrayMap extends AbstractMap<String, Object> implements Comparable<ArrayMap>, Writeable {
        final List<String> keys;
        final Comparable[] values;
        final List<DocValueFormat> formats;

        ArrayMap(StreamInput in) throws IOException {
            values = new Comparable[in.readVInt()];
            keys = Lists.newArrayListWithExpectedSize(values.length);
            formats = Lists.newArrayListWithExpectedSize(values.length);
            for (int i = 0; i < values.length; i++) {
                values[i] = (Comparable) in.readGenericValue();
                keys.add(in.readString());
                formats.add(DocValueFormat.RAW);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(values.length);
            for (int i=0; i<values.length; i++) {
                out.writeGenericValue(values[i]);
                out.writeString(keys.get(i));
            }
        }

        ArrayMap(ArrayMap other) {
            this.keys = Lists.newArrayList(other.keySet());
            this.values = Arrays.copyOf(other.values, other.values.length);
            this.formats = Lists.newArrayList(other.formats);
        }

        ArrayMap(List<String> keys, List<DocValueFormat> formats, Comparable[] values) {
            assert keys.size() == values.length && keys.size() == formats.size();
            this.keys = keys;
            this.formats = formats;
            this.values = values;
        }

        ArrayMap(List<String> keys, Comparable[] values) {
            assert keys.size() == values.length;
            this.keys = keys;
            this.formats = Lists.newArrayListWithCapacity(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                this.formats.add(DocValueFormat.RAW);
            }
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
