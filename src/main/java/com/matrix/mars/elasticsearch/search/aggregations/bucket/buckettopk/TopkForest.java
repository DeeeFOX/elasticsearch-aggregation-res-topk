package com.matrix.mars.elasticsearch.search.aggregations.bucket.buckettopk;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Set;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.stream.Collectors;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy.SKIP;

public class TopkForest {
    private final Map<InternalBucketTopk.ArrayMap, TopkTree> trees;
    private final List<String> baseKeyNames;
    private final int treeSize;
    private final int from;
    private final int size;
    private final FieldSortBuilder sort;

    public TopkForest(String[] baseKeyNames, int from, int size, FieldSortBuilder sort) {
        trees = new HashMap<>();
        this.baseKeyNames = Lists.newArrayList(baseKeyNames);
        this.treeSize = from + size;
        this.from = from;
        this.size = size;
        this.sort = sort;
    }

    /**
     * Add the {@link InternalBucketTopk.InternalBucket} type bucket of {@link BucketTopkPipelineAggregator}'s result(InternalBucketTopk)
     * From other shards
     *
     * @param it InternalBucketTopk.BucketIterator
     * @return Add successfully or not
     */
    public boolean addOthShardsBucket(InternalBucketTopk.BucketIterator it) {
        InternalBucketTopk.InternalBucket bucket = it.current;
        InternalBucketTopk.ArrayMap baseKeyValues = bucket.getBaseKeyValues();
        if (! trees.containsKey(baseKeyValues)) {
            trees.put(baseKeyValues, new TopkTree(baseKeyValues, treeSize, sort));
        }
        TopkTree tree = trees.get(baseKeyValues);
        TopkTree.TopkTreeNode treeNode = bucket.getOrgTokTreeNode();
        treeNode.setResBucket(bucket);
        return tree.addNode(treeNode);
    }

    /**
     * Collect the brother aggregation's {@link InternalMultiBucketAggregation} bucket values
     *
     * @param aggregations brother aggregations
     * @param bucketsPath buckets path to visit the value
     * @param parTopkTreeNode parent Topk Tree node to store the value
     * @param baseKeyValues base key names and key values
     * @param treeSize topk tree size(k)
     * @param sort tree sort order
     */
    public void collectBroAggsBucketValue(
            Aggregations aggregations, List<String> bucketsPath, TopkTree.TopkTreeNode parTopkTreeNode,
            InternalBucketTopk.ArrayMap baseKeyValues, int treeSize, FieldSortBuilder sort) {
        String keyName = bucketsPath.get(0);
        InternalBucketTopk.ArrayMap treeKeyValues = baseKeyValues;
        List<String> sublistedPath = bucketsPath.subList(1, bucketsPath.size());
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(keyName)) {
                // subPaths
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                if (bucketsPath.size() == 2) {
                    // bottom of aggregations
                    for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                        Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, sublistedPath, SKIP);
                        if (bucketValue != null && !Double.isNaN(bucketValue)) {
                            if (keyName.equals(baseKeyNames.get(0)) || treeKeyValues == null) {
                                treeKeyValues = new InternalBucketTopk.ArrayMap(baseKeyNames, new String[]{bucket.getKeyAsString()});
                            }
                            if (!trees.containsKey(treeKeyValues)) {
                                // no tree key added before
                                trees.put(treeKeyValues, new TopkTree(treeKeyValues, treeSize, sort));
                            }
                            InternalAggregation orgAgg = (InternalAggregation) bucket.getAggregations().asList().get(0);
                            if (null == parTopkTreeNode) {
                                TopkTree.TopkTreeNode childTreeNode = new TopkTree.TopkTreeNode(keyName, bucket.getKeyAsString());
                                childTreeNode.setProperties(bucket.getDocCount(), bucketValue, orgAgg);
                                trees.get(treeKeyValues).addNode(childTreeNode);
                            } else {
                                TopkTree.TopkTreeNode childTreeNode = parTopkTreeNode.deepCopy();
                                childTreeNode.putNodeKV(keyName, bucket.getKeyAsString());
                                childTreeNode.setProperties(bucket.getDocCount(), bucketValue, orgAgg);
                                trees.get(treeKeyValues).addNode(childTreeNode);
                            }
                        }
                    }
                } else {
                    // at top of aggregations
                    for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                        Aggregations subAggs = bucket.getAggregations();
                        String keyVal = bucket.getKeyAsString();
                        if (keyName.equals(baseKeyNames.get(0))) {
                            treeKeyValues = new InternalBucketTopk.ArrayMap(baseKeyNames, new String[]{bucket.getKeyAsString()});
                        }
                        if (null == parTopkTreeNode) {
                            this.collectBroAggsBucketValue(
                                    subAggs, sublistedPath,
                                    new TopkTree.TopkTreeNode(keyName, keyVal), treeKeyValues, treeSize, sort);
                        } else {
                            TopkTree.TopkTreeNode childTopkTreeNode = parTopkTreeNode.deepCopy();
                            childTopkTreeNode.putNodeKV(keyName, keyVal);
                            this.collectBroAggsBucketValue(
                                    subAggs, sublistedPath,
                                    childTopkTreeNode, treeKeyValues, treeSize, sort);
                        }
                    }
                }
            }
        }
    }

    public Collection<TopkTree> values() {
        return this.trees.values();
    }

    public Set<InternalBucketTopk.ArrayMap> keySet() {
        return this.trees.keySet();
    }

    public ArrayList<InternalBucketTopk.ArrayMap> sortedKeySet() {
        return trees.keySet().stream().sorted().collect(Collectors.toCollection(ArrayList::new));
    }
    public TopkTree get(InternalBucketTopk.ArrayMap key) {
        return this.trees.get(key);
    }

    public static class TopkTree {
        private final InternalBucketTopk.ArrayMap baseKeyValues;
        private final int maxTreeSize;
        private final FieldSortBuilder sort;
        private MinMaxPriorityQueue<TopkTreeNode> tree;

        public TopkTree(List<String> baseKeyNames, String[] baseKeyValues, int treeSize, FieldSortBuilder sort) {
            assert baseKeyNames.size() == baseKeyValues.length;
            this.baseKeyValues = new InternalBucketTopk.ArrayMap(baseKeyNames,baseKeyValues);
            this.maxTreeSize = treeSize;
            this.sort = sort;
        }

        public TopkTree(InternalBucketTopk.ArrayMap baseKeyValues, int treeSize, FieldSortBuilder sort) {
            this.baseKeyValues = new InternalBucketTopk.ArrayMap(baseKeyValues);
            this.maxTreeSize = treeSize;
            this.sort = sort;
        }

        public boolean addNode(TopkTreeNode treeNode) {
            if (null == tree) {
                tree = MinMaxPriorityQueue.orderedBy(
                        this.sort.order() == SortOrder.DESC ? new TopkTreeNodeComparator().reversed() : new TopkTreeNodeComparator())
                        .maximumSize(this.maxTreeSize)
                        .create();
            }
            return tree.add(treeNode);
        }

        public TopkTreeNode[] getNodes(int offset, int limit) {
            int actSize = tree.size() - offset;
            if (actSize <= 0) {
                return new TopkTreeNode[0];
            } else {
                Iterator<TopkTreeNode> it = tree.iterator();
                for (int i=offset; i>0; i--) {
                    assert it.hasNext();
                    it.next();
                }
                TopkTreeNode[] ret = new TopkTreeNode[Math.min(actSize, limit)];
                for (int i=0; i<limit && it.hasNext(); i++) {
                    ret[i] = it.next();
                }
                return ret;
            }
        }

        public List<InternalBucketTopk.InternalBucket> getNodesBuckets(int offset, int limit) {
            if (tree.size() < offset + limit) {
                return Lists.newArrayListWithCapacity(0);
            } else {
                Iterator<TopkTreeNode> it = tree.iterator();
                for (int i=offset; i>0; i--) {
                    if (!it.hasNext()) {
                        Lists.newArrayListWithCapacity(0);
                    }
                    it.next();
                }
                List<InternalBucketTopk.InternalBucket> ret = Lists.newArrayList();
                for (int i=0; i<limit && it.hasNext(); i++) {
                    TopkTreeNode node = it.next();
                    ret.add(node.getResBucket());
                }
                return ret;
            }
        }

        public InternalBucketTopk.ArrayMap getBaseKeyValues() {
            return baseKeyValues;
        }

        public int size(int offset) {
            if (null == tree) {
                return 0;
            } else {
                int validSize = tree.size() - offset;
                return Math.max(validSize, 0);
            }
        }

        public static class TopkTreeNode implements Comparable<TopkTreeNode>, Writeable {

            private final TreeMap<String, String> nodeKey;
            private Double fieldValue;
            private long docCount;
            private InternalAggregations resAggs;
            private InternalBucketTopk.InternalBucket resBucket;

            public TopkTreeNode() {
                this.nodeKey = new TreeMap<>();
            }

            public TopkTreeNode(String firstKeyName, String firstKeyValue) {
                this.nodeKey = new TreeMap<>();
                this.nodeKey.put(firstKeyName, firstKeyValue);
            }

            public TopkTreeNode(StreamInput in) throws IOException {
                int size = in.readVInt();
                nodeKey = new TreeMap<>();
                String key, value;
                for (int i=0; i<size; i++) {
                    key = in.readString();
                    value = in.readString();
                    nodeKey.put(key, value);
                }
                docCount = in.readVLong();
                fieldValue = in.readOptionalDouble();
                resAggs = new InternalAggregations(in);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeVInt(nodeKey.size());
                for (Map.Entry<String, String> entry: nodeKey.entrySet()) {
                    out.writeString(entry.getKey());
                    out.writeString(entry.getValue());
                }
                out.writeVLong(docCount);
                out.writeOptionalDouble(fieldValue);
                resAggs.writeTo(out);
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

            public void setProperties(long docCount, Double fieldValue, InternalAggregation orgAgg) {
                this.fieldValue = fieldValue;
                this.docCount = docCount;
                List<InternalAggregation> aggs = Lists.newArrayListWithCapacity(1);
                aggs.add(orgAgg);
                this.resAggs = new InternalAggregations(aggs);
            }

            public String putNodeKV(String keyName, String keyValue) {
                return this.nodeKey.put(keyName, keyValue);
            }

            public TopkTreeNode deepCopy() {
                TopkTreeNode copy = new TopkTreeNode();
                for (Map.Entry<String, String> entry : nodeKey.entrySet()) {
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
                for (String value : nodeKey.values()) {
                    values[i] = new BytesRef(value);
                    i++;
                }
                return new CompositeKey(values);
            }

            public InternalAggregations getResAggs() {
                return resAggs;
            }

            public void setResBucket(InternalBucketTopk.InternalBucket resBucket) {
                this.resBucket = resBucket;
            }

            public InternalBucketTopk.InternalBucket getResBucket() {
                return this.resBucket;
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
}
