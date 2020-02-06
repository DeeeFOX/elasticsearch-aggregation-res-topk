package com.matrix.mars.elasticsearch.search.aggregations.bucket;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy.SKIP;

public class TopkTree {
    private final String keyName;
    private final String keyValue;
    private final int size;
    private final FieldSortBuilder sort;
    private MinMaxPriorityQueue<TopkTreeNode> tree;

    public TopkTree(String keyName, String keyValue, int size, FieldSortBuilder sort) {
        this.keyName = keyName;
        this.keyValue = keyValue;
        this.size = size;
        this.sort = sort;
    }

    public boolean addNode(TopkTreeNode treeNode) {
        if (null == tree) {
            tree = MinMaxPriorityQueue.orderedBy(
                    this.sort.order() == SortOrder.DESC ? new TopkTreeNodeComparator().reversed() : new TopkTreeNodeComparator())
                    .maximumSize(this.size)
                    .create();
        }
        return tree.add(treeNode);
    }

    public TopkTreeNode[] getNodes() {
        return tree.toArray(new TopkTreeNode[tree.size()]);
    }

    public String getKey() {
        return keyName + "=" + keyValue;
    }

    public int size() {
        if (null == tree) {
            return 0;
        } else {
            return tree.size();
        }
    }

    public static void fromInternalAggregation(
            Map<String, TopkTree> topkForest,
            Aggregations aggregations, List<String> bucketsPath, TopkTreeNode parTopkTreeNode,
            String baseKeyName, String baseKeyValue, int treeSize, FieldSortBuilder sort) {
        String keyName = bucketsPath.get(0);
        String treeKey = baseKeyValue;
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {
                // subPaths
                List<String> sublistedPath = bucketsPath.subList(1, bucketsPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                if (bucketsPath.size() == 2) {
                    for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                        Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, sublistedPath, SKIP);
                        if (bucketValue != null && !Double.isNaN(bucketValue)) {
                            if (keyName.equals(baseKeyName) || baseKeyValue == null) {
                                treeKey = bucket.getKeyAsString();
                            }
                            if (!topkForest.containsKey(treeKey)) {
                                topkForest.put(treeKey, new TopkTree(keyName, treeKey, treeSize, sort));
                            }
                            if (null == parTopkTreeNode) {
                                TopkTreeNode childTreeNode = new TopkTreeNode(keyName, bucket.getKeyAsString());
                                childTreeNode.setProperties(bucket.getDocCount(), bucketValue, multiBucketsAgg);
                                topkForest.get(treeKey).addNode(childTreeNode);
                            } else {
                                TopkTreeNode childTreeNode = parTopkTreeNode.deepCopy();
                                childTreeNode.putNodeKV(keyName, bucket.getKeyAsString());
                                childTreeNode.setProperties(bucket.getDocCount(), bucketValue, multiBucketsAgg);
                                topkForest.get(treeKey).addNode(childTreeNode);
                            }
//                            collectBucketValue(bucket.getKeyAsString(), bucketValue);
                        }
                    }
                } else {
                    for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                        Aggregations subAggs = bucket.getAggregations();
                        String keyVal = bucket.getKeyAsString();
                        if (keyName.equals(baseKeyName) || baseKeyValue == null) {
                            treeKey = bucket.getKeyAsString();
                        }
                        if (null == parTopkTreeNode) {
                            fromInternalAggregation(topkForest, subAggs, sublistedPath,
                                    new TopkTreeNode(keyName, keyVal), baseKeyName, treeKey, treeSize, sort);
                        } else {
                            TopkTreeNode childTopkTreeNode = parTopkTreeNode.deepCopy();
                            childTopkTreeNode.putNodeKV(keyName, keyVal);
                            fromInternalAggregation(
                                    topkForest, subAggs, sublistedPath, childTopkTreeNode, baseKeyName, treeKey, treeSize, sort);
                        }
                    }
                }
            }
        }
    }

    public static class TopkTreeNode implements Comparable<TopkTreeNode> {

        private final TreeMap<String, String> nodeKey;
        private String fieldName;
        private Double fieldValue;
        private long docCount;
        private InternalAggregation orgAgg;

        public TopkTreeNode() {
            this.nodeKey = new TreeMap<>();
        }

        public TopkTreeNode(String firstKeyName, String firstKeyValue) {
            this.nodeKey = new TreeMap<>();
            this.nodeKey.put(firstKeyName, firstKeyValue);
        }

        @Override
        public int compareTo(TopkTreeNode that) {
            if (this.fieldValue == null && that.getFieldValue() == null) {
                return 0;
            } else if (this.fieldValue == null) {
                return 1;
            } else if (that.getFieldValue() == null) {
                return -1;
            } else {
                return fieldValue.compareTo(that.getFieldValue());
            }
        }

        public Double getFieldValue() {
            return fieldValue;
        }

        public long getDocCount() {
            return docCount;
        }

        public InternalAggregation getOrgAgg() {
            return orgAgg;
        }

        public void setProperties(long docCount, Double fieldValue, InternalAggregation orgAgg) {
            this.fieldValue = fieldValue;
            this.docCount = docCount;
            this.orgAgg = orgAgg;
        }

        public String putNodeKV(String keyName, String keyValue) {
            return this.nodeKey.put(keyName, keyValue);
        }

        public TopkTreeNode deepCopy() {
            TopkTreeNode copy = new TopkTreeNode();
            for (Map.Entry<String, String> entry : copy.nodeKey.entrySet()) {
                copy.putNodeKV(entry.getKey(), entry.getValue());
            }
            return copy;
        }

        /**
         * Builds the {@link CompositeKey} for <code>slot</code>.
         */
        @SuppressWarnings("rawtypes")
        public CompositeKey getCompositeKey() {
            Comparable[] values = new Comparable[nodeKey.size()];
            int i = 0;
            for (Map.Entry<String, String> entry : this.nodeKey.entrySet()) {
                values[i] = new BytesRef(entry.getKey() + entry.getValue());
                i++;
            }
            return new CompositeKey(values);
        }
    }

    /**
     * The comparator class of TopkTreeNode
     */
    public static class TopkTreeNodeComparator implements Comparator<TopkTreeNode> {

        @Override
        public int compare(TopkTreeNode o1, TopkTreeNode o2) {
            if (o1.getFieldValue() == null && o2.getFieldValue() == null) {
                return 0;
            } else if (o1.getFieldValue() == null) {
                return 1;
            } else if (o2.getFieldValue() == null) {
                return -1;
            } else {
                return o1.getFieldValue().compareTo(o2.getFieldValue());
            }
        }
    }
}