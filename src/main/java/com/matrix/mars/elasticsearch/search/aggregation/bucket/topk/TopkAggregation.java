package com.matrix.mars.elasticsearch.search.aggregation.bucket.topk;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Inspired by CompositeAggregation
 */
public interface TopkAggregation extends MultiBucketsAggregation {
    interface Bucket extends MultiBucketsAggregation.Bucket {
        Map<String, Object> getKey();
    }

    @Override
    List<? extends TopkAggregation.Bucket> getBuckets();


    static void buildCompositeMap(String fieldName, Map<String, Object> keyValues, XContentBuilder builder) throws IOException {
        builder.startObject(fieldName);
        for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
    }

    static XContentBuilder bucketToXContent(TopkAggregation.Bucket bucket,
                                            XContentBuilder builder, Params params) throws IOException {
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
    static XContentBuilder toXContentFragment(TopkAggregation aggregation, XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (TopkAggregation.Bucket bucket : aggregation.getBuckets()) {
            bucketToXContent(bucket, builder, params);
        }
        builder.endArray();
        return builder;
    }
}
