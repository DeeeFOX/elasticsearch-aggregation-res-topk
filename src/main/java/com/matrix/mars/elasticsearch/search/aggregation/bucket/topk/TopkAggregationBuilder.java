package com.matrix.mars.elasticsearch.search.aggregation.bucket.topk;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * TODO
 * Not implement yet
 */
public class TopkAggregationBuilder extends AbstractAggregationBuilder<TopkAggregationBuilder> {
    public static final String NAME = "topk";

    public static final ParseField FROM_FIELD_NAME = new ParseField("from");
    public static final ParseField SIZE_FIELD_NAME = new ParseField("size");
    public static final ParseField ORDER_FIELD_NAME = new ParseField("order");

    public TopkAggregationBuilder(String name) {
        super(name);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {

    }

    @Override
    protected AggregatorFactory doBuild(
            SearchContext context, AggregatorFactory parent, AggregatorFactories.Builder subfactoriesBuilder) throws IOException {
        return null;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return null;
    }

    @Override
    public String getType() {
        return null;
    }
}
