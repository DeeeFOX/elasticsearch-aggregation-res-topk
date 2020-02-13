package com.matrix.mars.elasticsearch.search.aggregations.bucket.buckettopk;

import com.google.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.BucketMetricsPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * SiblingPipline (brother pipeline) Aggregator
 * Strongly inspired by {@link MaxBucketPipelineAggregator}
 * And inspired by {@link BucketMetricsPipelineAggregator}
 * But {@link BucketMetricsPipelineAggregator} only return one metric bucket
 * So we create the BucketTopkPipelineAggregator to do our top k filter job
 */
public class BucketTopkPipelineAggregator extends SiblingPipelineAggregator {

    // Aggregator's parameters
    private final int from;
    private final int size;
    private final String baseKeyName;
    private final FieldSortBuilder sort;

     // Aggregator's attribution
    private TopkForest topkForest; // Key by baseKeyName's value
    private final List<String> bucketsPath;
    private final int treeSize;
    /**
     * CompositeKey parameters
     */
    private final List<String> keyNames;
    private final List<DocValueFormat> keyFormats;
    private final int[] keyReverseMuls;

    /**
     * Read from a stream.
     * Should follow the parameters sequence
     * For use of the es nodes' internal communications
     */
    public BucketTopkPipelineAggregator(StreamInput in) throws IOException {
        super(in.readString(), in.readStringArray(), in.readMap());
        from = in.readVInt();
        size = in.readVInt();
        baseKeyName = in.readString();
        sort = new FieldSortBuilder(in);
        String[] bp = sort.getFieldName().split(">");
        if (bp.length <= 1) {
            throw new IllegalArgumentException(
                    "[" + BucketTopkPipelineAggregationBuilder.NAME
                            + "] only supports bucket path sorting; incompatible sort path length: ["
                            + bp.length + "]");
        }
        keyNames = Arrays.asList(Arrays.copyOf(bp, bp.length - 1));
        keyFormats = Lists.newArrayListWithCapacity(keyNames.size());
        keyReverseMuls = new int[keyNames.size()];
        for (int i = 0; i < keyNames.size(); i++) {
            keyFormats.add(DocValueFormat.RAW);
            keyReverseMuls[i] = 1;
        }
        bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        treeSize = from + size;
    }

    /**
     * Write to a stream.
     * Should follow the parameters sequence
     * For use of the es nodes' internal communications
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeString(baseKeyName);
        sort.writeTo(out);

    }

    /**
     * Construct the aggregator from the {@link BucketTopkPipelineAggregationBuilder}.
     * Should follow the parameters sequence
     */
    protected BucketTopkPipelineAggregator(
            String name, String[] bucketsPaths, Map<String, Object> metaData, int from, int size,
            String baseKeyName, FieldSortBuilder sort) {
        super(name, bucketsPaths, metaData);
        this.from = from;
        this.size = size;
        this.baseKeyName = baseKeyName;
        this.sort = sort;
        String[] bp = sort.getFieldName().split(">");
        if (bp.length <= 1) {
            throw new IllegalArgumentException(
                    "[" + BucketTopkPipelineAggregationBuilder.NAME
                            + "] only supports bucket path sorting; incompatible sort path length: ["
                            + bp.length + "]");
        }
        keyNames = Arrays.asList(Arrays.copyOf(bp, bp.length - 1));
        keyFormats = Lists.newArrayListWithCapacity(keyNames.size());
        keyReverseMuls = new int[keyNames.size()];
        for (int i = 0; i < keyNames.size(); i++) {
            keyFormats.add(DocValueFormat.RAW);
            keyReverseMuls[i] = 1;
        }
        bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        treeSize = from + size;
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
        topkForest = new TopkForest(new String[]{baseKeyName}, from, size, sort);
    }

    /**
     * Called after a collectBucketValue run is finished to build the aggregation
     * return as a result of the collected state handler.
     * Inspired by CompositeAggregator
     */
    protected InternalBucketTopk buildAggregation() {
        int num = 0;
        for (TopkForest.TopkTree tree : this.topkForest.values()) {
            num += tree.size(from);
        }
        final InternalBucketTopk.InternalBucket[] buckets = new InternalBucketTopk.InternalBucket[num];
        int i = 0;
        for (InternalBucketTopk.ArrayMap key : this.topkForest.keySet().stream().sorted().collect(
                Collectors.toCollection(ArrayList::new))) {
            TopkForest.TopkTree tree = this.topkForest.get(key);
            for (TopkForest.TopkTree.TopkTreeNode treeNode : tree.getNodes(from , size)) {
                buckets[i] = new InternalBucketTopk.InternalBucket(
                        keyNames, keyFormats, treeNode.getCompositeKey(), keyReverseMuls, treeNode.getDocCount(),
                        treeNode.getResAggs(), tree.getBaseKeyValues(), treeNode);
                i++;
            }
        }
        // TODO
        // May be we should give some pipeline aggregator from any method
        return new InternalBucketTopk(
                name(), num, keyNames, keyFormats, Arrays.asList(buckets), keyReverseMuls,
                Lists.newArrayListWithCapacity(0), metaData(), from, size, baseKeyName, sort);
    }

    /**
     * Collect each brother aggregations
     * So the state can be add to the TopkForest
     * Which based on the key and metric value for this bucket
     */
    protected void collectBucketValue(Aggregations aggregations) {
        topkForest.collectBroAggsBucketValue(aggregations, bucketsPath, null, null, treeSize, sort);
    }


    /**
     * Do the reduce work at {@link InternalAggregations}::reduce when context.isFinalReduce
     * Or the reduce work at {@link SiblingPipelineAggregator}::reduce
     * Like what {@link BucketMetricsPipelineAggregator} do
     * This reduce works start at this sibling(here topk_bucket) pipeline layer with given brother aggregations param
     *
     * Like handle the aggs of key1, key2, key3
     * {
     *     "aggs": {
     *         "topk": {},
     *         "key1": {
     *             "terms": {},
     *             "aggs": {
     *                 "ts": {
     *                     "date_histogram": {},
     *                     "aggs": {
     *                         "res": {
     *                             "sum": {}
     *                         }
     *                     }
     *                 }
     *             }
     *         },
     *         "key2": { ... },
     *         "key3": { ... }
     *     }
     * }
     *
     * Which will handle the result of all these brother aggregations through the buckets path
     * Finally reduce into and return the InternalAggregation defined of the aggregator(here {@link InternalBucketTopk})
     * As the this aggregator's result
     *
     * @param brotherAggregations sibling(brother) aggregations to reduce
     * @param context reduce context
     * @return {@link InternalBucketTopk}
     */
    @Override
    public InternalAggregation doReduce(Aggregations brotherAggregations, InternalAggregation.ReduceContext context) {
        preCollection();
        collectBucketValue(brotherAggregations);
        return buildAggregation();
    }

    /**
     * Do the reduce work at {@link InternalAggregation}::reduce when reduceContext.isFinalReduce
     *
     * This reduce works called by the parent aggregator
     * And run at this sibling(here topk_bucket) pipeline's parent layer with given brother aggregations param
     *
     * Like handle all the topk aggs in key0
     * {
     *     "aggs": {
     *         "key0": {
     *             "terms": {},
     *             "aggs": {
     *                 "topk": {},
     *                 "key1": {
     *                     "terms": {},
     *                     "aggs": {
     *                         "ts": {
     *                             "date_histogram": {},
     *                             "aggs": {
     *                                 "res": {
     *                                     "sum": {}
     *                                 }
     *                             }
     *                         }
     *                     }
     *                 },
     *                 "key2": { ... },
     *                 "key3": { ... }
     *             }
     *         }
     *     }
     * }
     *
     * Which will handle the parent InternalAggregation defined of the aggregator(here {@link InternalBucketTopk}) like key0
     *
     * Here the same to {@link SiblingPipelineAggregator}::reduce do
     *
     * @param parentAggregation this aggregator itself's parent aggregation
     * @param reduceContext reduce context
     * @return {@link InternalAggregation}
     */
//    @SuppressWarnings("unchecked")
//    @Override
    public InternalAggregation explainWhatFunction_reduce_Do(
            InternalAggregation parentAggregation, InternalAggregation.ReduceContext reduceContext) {
        preCollection();
//        if (aggregation instanceof InternalMultiBucketAggregation) {
//            @SuppressWarnings("rawtypes")
//            InternalMultiBucketAggregation multiBucketsAgg = (InternalMultiBucketAggregation) aggregation;
//            List<? extends MultiBucketsAggregation.Bucket> buckets = multiBucketsAgg.getBuckets();
//            for (MultiBucketsAggregation.Bucket bucket1 : buckets) {
//                InternalMultiBucketAggregation.InternalBucket bucket = (InternalMultiBucketAggregation.InternalBucket) bucket1;
//                collectBucketValue(bucket.getAggregations());
//            }
//        } else if (aggregation instanceof InternalSingleBucketAggregation) {
//            InternalSingleBucketAggregation singleBucketAgg = (InternalSingleBucketAggregation) aggregation;
//            collectBucketValue(singleBucketAgg.getAggregations());
//        } else {
//            throw new IllegalStateException("Aggregation [" + aggregation.getName() + "] must be a bucket aggregation ["
//                    + aggregation.getWriteableName() + "]");
//        }
        return buildAggregation();
    }
}
