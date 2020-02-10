package com.matrix.mars.elasticsearch.plugin;

import com.matrix.mars.elasticsearch.search.aggregations.bucket.buckettopk.BucketTopkPipelineAggregationBuilder;
import com.matrix.mars.elasticsearch.search.aggregations.bucket.buckettopk.BucketTopkPipelineAggregator;
import com.matrix.mars.elasticsearch.search.aggregations.bucket.buckettopk.InternalBucketTopk;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Entry and description to elasticsearch of this plugin
 * Bound with file content:
 *
 * build.gradle:
 *
 * esplugin {
 *     name 'elasticsearch-aggregation-res-topk'
 *     description 'Sort and choose top K to return of aggregations result'
 *     classname 'com.matrix.mars.elasticsearch.plugin.TopkAggregation' // plugin description class path
 *     licenseFile = rootProject.file('LICENSE')
 *     noticeFile = rootProject.file('README.md')
 * }
 *
 */
public class TopkAggregation extends Plugin implements SearchPlugin {
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
