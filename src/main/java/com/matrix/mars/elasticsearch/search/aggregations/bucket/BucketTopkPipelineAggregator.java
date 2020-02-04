package com.matrix.mars.elasticsearch.search.aggregations.bucket;

import com.google.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.*;

public class BucketTopkPipelineAggregator extends PipelineAggregator {
    private final int from;
    private final Integer size;
    private final BucketHelpers.GapPolicy gapPolicy;
    private final String baseKeyName;
    private final FieldSortBuilder sort;
    private final DocValueFormat format;
    private Map<String, TopkTree> topkForest;

    /**
     * Read from a stream.
     * Should follow the parameters sequence
     */
    public BucketTopkPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        from = in.readVInt();
        size = in.readOptionalVInt();
        gapPolicy = BucketHelpers.GapPolicy.readFrom(in);
        baseKeyName = in.readString();
        sort = in.readList(FieldSortBuilder::new).get(0);
        this.format = new DocValueFormat.Decimal("###.###");
    }

    protected BucketTopkPipelineAggregator(
            String name, String[] bucketsPaths, Map<String, Object> metaData, int from, Integer size, BucketHelpers.GapPolicy gapPolicy,
            String baseKeyName, List<FieldSortBuilder> sorts) {
        super(name, bucketsPaths, metaData);
        this.from = from;
        this.size = size;
        this.gapPolicy = gapPolicy;
        this.baseKeyName = baseKeyName;
        this.sort = sorts.get(0);
        this.format = new DocValueFormat.Decimal("###.###");
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeOptionalVInt(size);
        gapPolicy.writeTo(out);
        out.writeString(baseKeyName);
        List<FieldSortBuilder> sorts = new ArrayList<>();
        sorts.add(this.sort);
        out.writeList(sorts);

    }

    @Override
    public String getWriteableName() {
        return BucketTopkPipelineAggregationBuilder.NAME;
    }

    /**
     * Called before initial collection and between successive collection runs.
     * A chance to initialize or re-initialize state
     */
    protected void preCollection() {
        topkForest = new HashMap<>();
    }

    /**
     * Called after a collection run is finished to build the aggregation for
     * the collected state.
     * Inspired by CompositeAggregator
     */
    protected InternalAggregation buildAggregation() {
        int num = 0;
        for (TopkTree tree : this.topkForest.values()) {
            num += tree.size();
        }
        final InternalBucketTopk.InternalBucket[] buckets = new InternalBucketTopk.InternalBucket[num];
        int i = 0;
        int reverseMul = sort.order() == SortOrder.ASC ? 1 : -1;
        int[] reverseMuls = new int[1];
        reverseMuls[0] = reverseMul;
        List<String > sourceNames = Lists.newArrayListWithCapacity(1);
        sourceNames.add(baseKeyName);
        List<DocValueFormat> formats = Lists.newArrayListWithCapacity(1);
        formats.add(format);
        for(TopkTree tree: this.topkForest.values()) {

            for (TopkTree.TopkTreeNode treeNode: tree.getNodes()) {
                List<InternalAggregation> aggs = Lists.newArrayListWithCapacity(1);
                aggs.add(treeNode.getOrgAgg());
                buckets[i] = new InternalBucketTopk.InternalBucket(sourceNames, formats, treeNode.getCompositeKey(), reverseMuls, treeNode.getDocCount(), new InternalAggregations(aggs));
                i++;
            }
        }
        CompositeKey lastBucket = num > 0 ? buckets[num-1].getRawKey() : null;
        return new InternalBucketTopk(
                baseKeyName, num, sourceNames, formats, Arrays.asList(buckets), lastBucket, reverseMuls, Lists.newArrayListWithCapacity(0), metaData());
    }

    /**
     * Called for each bucket with a value so the state can be modified based on
     * the key and metric value for this bucket
     */
    protected void collectBucketValue(int treeSize, Aggregations aggregations, List<String> bucketsPath) {
        TopkTree.fromInternalAggregation(topkForest, aggregations, bucketsPath, null, gapPolicy, baseKeyName, null, treeSize, sort);
    }


    public InternalAggregation doReduce(Aggregations aggregations, InternalAggregation.ReduceContext context) {
        // sibling max bucket
        preCollection();
        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        int treeSize = from + size;
        collectBucketValue(treeSize, aggregations, bucketsPath);
        return buildAggregation();
    }

    @SuppressWarnings("unchecked")
    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, InternalAggregation.ReduceContext reduceContext) {
        // sibling max bucket
        if (aggregation instanceof InternalMultiBucketAggregation) {
            @SuppressWarnings("rawtypes")
            InternalMultiBucketAggregation multiBucketsAgg = (InternalMultiBucketAggregation) aggregation;
            List<? extends MultiBucketsAggregation.Bucket> buckets = multiBucketsAgg.getBuckets();
            List<MultiBucketsAggregation.Bucket> newBuckets = new ArrayList<>();
            for (MultiBucketsAggregation.Bucket bucket1 : buckets) {
                InternalMultiBucketAggregation.InternalBucket bucket = (InternalMultiBucketAggregation.InternalBucket) bucket1;
                InternalAggregation aggToAdd = doReduce(bucket.getAggregations(), reduceContext);
                List<InternalAggregation> aggs = new ArrayList<>();
                aggs.add(aggToAdd);
                InternalMultiBucketAggregation.InternalBucket newBucket = multiBucketsAgg.createBucket(new InternalAggregations(aggs),
                        bucket);
                newBuckets.add(newBucket);
            }
            return multiBucketsAgg.create(newBuckets);
        } else if (aggregation instanceof InternalSingleBucketAggregation) {
            InternalSingleBucketAggregation singleBucketAgg = (InternalSingleBucketAggregation) aggregation;
            InternalAggregation aggToAdd = doReduce(singleBucketAgg.getAggregations(), reduceContext);
            List<InternalAggregation> aggs = new ArrayList<>();
            aggs.add(aggToAdd);
            return singleBucketAgg.create(new InternalAggregations(aggs));
        } else {
            throw new IllegalStateException("Aggregation [" + aggregation.getName() + "] must be a bucket aggregation ["
                    + aggregation.getWriteableName() + "]");
        }
    }
}
