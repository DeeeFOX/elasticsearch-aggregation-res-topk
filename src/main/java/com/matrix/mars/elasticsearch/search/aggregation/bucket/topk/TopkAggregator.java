package com.matrix.mars.elasticsearch.search.aggregation.bucket.topk;

import com.google.common.collect.Lists;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.RoaringDocIdSet;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.MultiBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Collections;

/**
 * TopkAggregator that select top k result bucket from the children bucket and return
 */
public class TopkAggregator extends BucketsAggregator {

    // parameters of topk aggregator
    private final int from;
    private final int size;
    private final String baseKeyName;
    private final FieldSortBuilder sort;

    // stateful object to help collect data from children aggregator
    private final List<Entry> entries = new ArrayList<>();
    private LeafReaderContext currentLeaf;
    private RoaringDocIdSet.Builder docIdSetBuilder;
    private BucketCollector deferredCollectors;

    // key name parameters
    private final List<String> sourceNames;
    private final int[] sourceReverseMuls;
    private final List<DocValueFormat> sourceFormats;


    public TopkAggregator(
            String name, AggregatorFactories factories, SearchContext context,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
            int from, int size, String baseKeyName, FieldSortBuilder sort
    ) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.from = from;
        this.size = size;
        this.baseKeyName = baseKeyName;
        this.sort = sort;
        String[] sns = sort.getFieldName().split(">");
        this.sourceNames = Arrays.asList(sns);
        this.sourceReverseMuls = new int[sns.length];
        Arrays.fill(this.sourceReverseMuls, 1);
        this.sourceFormats = Lists.newArrayListWithCapacity(sns.length);
        for (int i = 0; i < sns.length; i++) {
            this.sourceFormats.add(DocValueFormat.RAW);
        }
    }

    private void finishLeaf() {
        // TODO
        // still need to know what is finish leaf
        if (currentLeaf != null) {
            DocIdSet docIdSet = docIdSetBuilder.build();
            entries.add(new TopkAggregator.Entry(currentLeaf, docIdSet));
            currentLeaf = null;
            docIdSetBuilder = null;
        }
    }

    /**
     * Get a {@link LeafBucketCollector} for the given ctx, which should
     * delegate to the given collector.
     * <p>
     * The collector collects the docs, including or not some score (depending of the including of a Scorer) in the
     * collect() process.
     * <p>
     * The LeafBucketCollector is a "Per-leaf bucket collector". It collects docs for the account of buckets.
     */
    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        finishLeaf();
        // TODO
        // not finished yet
        return null;
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        return null;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTopk(name, from, size, sourceNames, sourceFormats, Collections.emptyList(), sourceReverseMuls,
                pipelineAggregators(), metaData());
    }

    @Override
    protected void doPreCollection() throws IOException {
        List<BucketCollector> collectors = Arrays.asList(subAggregators);
        deferredCollectors = MultiBucketCollector.wrap(collectors);
        collectableSubAggregators = BucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    protected void doPostCollection() throws IOException {
        finishLeaf();
    }

    /**
     * TODO
     * check if useless
     * Inspired by CompositeAggregator.Entry
     * Maybe not useful
     */
    private static class Entry {
        final LeafReaderContext context;
        final DocIdSet docIdSet;

        Entry(LeafReaderContext context, DocIdSet docIdSet) {
            this.context = context;
            this.docIdSet = docIdSet;
        }
    }
}
