package com.matrix.mars.elasticsearch.search.aggregations.bucket.buckettopk;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A multi bucket aggregation which returns topk buckets sorted by return value.
 * The aggregation will result in flatted buckets as what Composistion originally likes.
 * Strongly inspired by {@link org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation}.
 */

public interface BucketTopkAggregation extends MultiBucketsAggregation {

    interface Bucket extends MultiBucketsAggregation.Bucket {
        Map<String, Object> getKey();
    }

    @Override
    List<? extends BucketTopkAggregation.Bucket> getBuckets();

    static void buildCompositeMap(String fieldName, Map<String, Object> keyValues, XContentBuilder builder) throws IOException {
        builder.startObject(fieldName);
        for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
    }

    static XContentBuilder bucketToXContent(
            BucketTopkAggregation.Bucket bucket, XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        buildCompositeMap(CommonFields.KEY.getPreferredName(), bucket.getKey(), builder);
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), bucket.getDocCount());
        bucket.getAggregations().toXContentInternal(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Create Aggregation XContent
     */
    static XContentBuilder toXContentFragment(
            BucketTopkAggregation aggregation, XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (BucketTopkAggregation.Bucket bucket : aggregation.getBuckets()) {
            bucketToXContent(bucket, builder, params);
        }
        builder.endArray();
        return builder;
    }
}
