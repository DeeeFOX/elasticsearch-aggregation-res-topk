package com.matrix.mars.elasticsearch.plugin;

import com.matrix.mars.elasticsearch.search.aggregations.bucket.BucketTopkPipelineAggregationBuilder;
import com.matrix.mars.elasticsearch.search.aggregations.bucket.BucketTopkPipelineAggregator;
import com.matrix.mars.elasticsearch.search.aggregations.bucket.InternalBucketTopk;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;
import java.util.List;

public class BucketTopkAggregation extends Plugin implements SearchPlugin {
    @Override
    public List<SearchPlugin.PipelineAggregationSpec> getPipelineAggregations() {
        ArrayList<SearchPlugin.PipelineAggregationSpec> r = new ArrayList<>();
        r.add(
                new PipelineAggregationSpec(
                        BucketTopkPipelineAggregationBuilder.NAME,
                        BucketTopkPipelineAggregationBuilder::new,
                        BucketTopkPipelineAggregator::new,
                        BucketTopkPipelineAggregationBuilder::parse)
                        .addResultReader(InternalBucketTopk::new));
        return r;
    }
}
