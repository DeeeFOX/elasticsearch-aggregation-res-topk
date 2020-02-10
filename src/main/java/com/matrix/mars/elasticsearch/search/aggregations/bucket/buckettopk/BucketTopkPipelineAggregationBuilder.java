package com.matrix.mars.elasticsearch.search.aggregations.bucket.buckettopk;

import com.google.common.collect.Lists;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Builds a pipeline aggregation that allows sorting the buckets of its parent
 * aggregation. The sub-aggregations result value will be used as sort keys.
 * Parameters {@code from} and {@code size} may also be set in order to truncate the result bucket list.
 * Strongly Inspired by {@link org.elasticsearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder}
 */
public class BucketTopkPipelineAggregationBuilder extends BucketMetricsPipelineAggregationBuilder<BucketTopkPipelineAggregationBuilder> {
    // aggregation name
    public static final String NAME = "bucket_topk";
    // aggregation params
    private static final ParseField FROM = new ParseField("from");
    private static final ParseField SIZE = new ParseField("size");
    private static final ParseField BASE_KEY_NAME = new ParseField("base_key_name");
    private static final ParseField SORT = new ParseField("sort");

    private int from = 0;
    private int size = 1;
    private String baseKeyName;
    private FieldSortBuilder sort;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<BucketTopkPipelineAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
            NAME, false, (a, context) -> new BucketTopkPipelineAggregationBuilder(context, (FieldSortBuilder) a[0]));

    static {
        PARSER.declareInt(BucketTopkPipelineAggregationBuilder::from, FROM);
        PARSER.declareInt(BucketTopkPipelineAggregationBuilder::size, SIZE);
        PARSER.declareString(BucketTopkPipelineAggregationBuilder::baseKeyName, BASE_KEY_NAME);
        PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> {
                    List<SortBuilder<?>> sorts = FieldSortBuilder.fromXContent(p);
                    if (sorts.size() != 1) {
                        throw new IllegalArgumentException(
                                "[" + NAME + "] only supports exactly 1 parameter given, but incompatible sort size: " + sorts.size());
                    }
                    SortBuilder<?> sort = sorts.get(0);
                    if (sort instanceof FieldSortBuilder == false) {
                        throw new IllegalArgumentException("[" + NAME + "] only supports field based sorting; incompatible sort: ["
                                + sort + "]");
                    }
                    return (FieldSortBuilder) sort;
                },
                SORT,
                ObjectParser.ValueType.OBJECT_OR_NULL);
    }

    /**
     * Read from PARSER
     * @param name Name of the aggregator
     * @param sort The only one parameters that parsed by declareField
     */
    public BucketTopkPipelineAggregationBuilder(
            String name, FieldSortBuilder sort) {
        super(name, NAME, new String[]{sort.getFieldName()});
        this.sort = sort;
    }

    /**
     * Read from a stream.
     */
    public BucketTopkPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        from = in.readVInt();
        size = in.readOptionalVInt();
        baseKeyName = in.readString();
        sort = new FieldSortBuilder(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeOptionalVInt(size);
        out.writeString(baseKeyName);
        sort.writeTo(out);
    }

    public BucketTopkPipelineAggregationBuilder from(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("[" + FROM.getPreferredName() + "] must be a non-negative integer: [" + from + "]");
        }
        this.from = from;
        return this;
    }

    public BucketTopkPipelineAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[" + SIZE.getPreferredName() + "] must be a positive integer: [" + size + "]");
        }
        this.size = size;
        return this;
    }

    public BucketTopkPipelineAggregationBuilder baseKeyName(String baseKeyName) {
        this.baseKeyName = baseKeyName;
        return this;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
        return new BucketTopkPipelineAggregator(name, bucketsPaths, metaData, from, size, baseKeyName, sort);
    }

    @Override
    public void doValidate(AggregatorFactory parent, Collection<AggregationBuilder> aggFactories,
                           Collection<PipelineAggregationBuilder> pipelineAggregatoractories) {
        if (sort == null && size <= 0 && from < 0) {
            throw new IllegalStateException("[" + name + "] is configured to perform nothing. Please set either of "
                    + Arrays.asList(SORT.getPreferredName(), SIZE.getPreferredName(), FROM.getPreferredName())
                    + " to use " + NAME);
        }
    }

    /**
     * It should follow the sequence declare at the PARSER
     * @param builder 创建参数builder
     * @param params 传入参数集合
     * @return XContentBuilder
     * @throws IOException From XContentBuilder::field
     */
    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(FROM.getPreferredName(), from);
        builder.field(SIZE.getPreferredName(), size);
        builder.field(BASE_KEY_NAME.getPreferredName(), baseKeyName);
        builder.field(SORT.getPreferredName(), sort);
        return builder;
    }

    public static BucketTopkPipelineAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, reducerName);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sort, from, size);
    }

    @Override
    public boolean equals(Object obj) {
        List<FieldSortBuilder> sorts = Lists.newArrayListWithCapacity(1);
        sorts.add(sort);
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        BucketTopkPipelineAggregationBuilder other = (BucketTopkPipelineAggregationBuilder) obj;
        List<FieldSortBuilder> otherSorts = Lists.newArrayListWithCapacity(1);
        otherSorts.add(other.sort);
        return Objects.equals(sorts, otherSorts)
                && Objects.equals(from, other.from)
                && Objects.equals(size, other.size);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
