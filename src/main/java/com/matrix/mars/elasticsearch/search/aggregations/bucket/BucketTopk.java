package com.matrix.mars.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A multi bucket aggregation which returns topk buckets sorted by return value.
 * The aggregation will result in nested buckets as what it originally likes.
 * Strongly inspired by {@link org.elasticsearch.search.aggregations.pipeline.BucketSortPipelineAggregator}.
 */

public interface BucketTopk extends MultiBucketsAggregation {
    interface Bucket extends MultiBucketsAggregation.Bucket {}

    @Override
    List<? extends Bucket> getBuckets();
}
