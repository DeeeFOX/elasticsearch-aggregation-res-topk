package com.matrix.mars.elasticsearch.search.aggregations.bucket;

import com.google.common.collect.Lists;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Builds a pipeline aggregation that allows sorting the buckets of its parent
 * aggregation. The bucket {@code _key}, {@code _count} or sub-aggregations may be used as sort
 * keys. Parameters {@code from} and {@code size} may also be set in order to truncate the
 * result bucket list.
 */
public class BucketTopkPipelineAggregationBuilder extends BucketMetricsPipelineAggregationBuilder<BucketTopkPipelineAggregationBuilder> {
    // aggregation name
    public static final String NAME = "bucket_topk";
    // aggregation params
    private static final ParseField FROM = new ParseField("from");
    private static final ParseField SIZE = new ParseField("size");
    private static final ParseField BASE_KEY_NAME = new ParseField("baseKeyName");
    private static final ParseField SORT = new ParseField("sort");

    private int from = 0;
    private Integer size;
    private String baseKeyName;
    private FieldSortBuilder sort;
    private DocValueFormat format;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<BucketTopkPipelineAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(NAME,
            false, (a, context) -> new BucketTopkPipelineAggregationBuilder(context, (List<FieldSortBuilder>) a[0]));
//            (int) a[0], (Integer) a[1], (String) a[2], (List<FieldSortBuilder>) a[3]));

    static {
        PARSER.declareInt(BucketTopkPipelineAggregationBuilder::from, FROM);
        PARSER.declareInt(BucketTopkPipelineAggregationBuilder::size, SIZE);
        PARSER.declareString(BucketTopkPipelineAggregationBuilder::baseKeyName, BASE_KEY_NAME);
        PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> {
                    List<SortBuilder<?>> sorts = SortBuilder.fromXContent(p);
                    List<FieldSortBuilder> fieldSorts = new ArrayList<>(sorts.size());
                    for (SortBuilder<?> sort : sorts) {
                        if (sort instanceof FieldSortBuilder == false) {
                            throw new IllegalArgumentException("[" + NAME + "] only supports field based sorting; incompatible sort: ["
                                    + sort + "]");
                        }
                        fieldSorts.add((FieldSortBuilder) sort);
                    }
                    return fieldSorts;
                },
                SORT,
                ObjectParser.ValueType.OBJECT_ARRAY);
    }

    /**
     * Read from PARSER
     * @param name Name of the aggregator
     * @param sorts The only one parameters that parsed by declareField
     */
    public BucketTopkPipelineAggregationBuilder(
            String name, List<FieldSortBuilder> sorts) {
        super(name, NAME, sorts == null ? new String[0] : sorts.stream().map(s -> s.getFieldName()).toArray(String[]::new));
        this.sort = sorts == null || sorts.size()== 0 ? null : sorts.get(0);
        this.format = new DocValueFormat.Decimal("###.###");
    }

    /**
     * Read from a stream.
     */
    public BucketTopkPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        from = in.readVInt();
        size = in.readOptionalVInt();
        baseKeyName = in.readString();
        sort = in.readList(FieldSortBuilder::new).get(0);
        format = new DocValueFormat.Decimal("###.###");
    }

//    @Override
//    protected void doWriteTo(StreamOutput out) throws IOException {
//        out.writeVInt(from);
//        out.writeOptionalVInt(size);
//        out.writeString(baseKeyName);
//        List<FieldSortBuilder> sorts = new ArrayList<>();
//        sorts.add(this.sort);
//        out.writeList(sorts);
//    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeOptionalVInt(size);
        out.writeString(baseKeyName);
        List<FieldSortBuilder> sorts = new ArrayList<>();
        sorts.add(this.sort);
        out.writeList(sorts);
    }

    public BucketTopkPipelineAggregationBuilder from(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("[" + FROM.getPreferredName() + "] must be a non-negative integer: [" + from + "]");
        }
        this.from = from;
        return this;
    }

    public BucketTopkPipelineAggregationBuilder size(Integer size) {
        if (size != null && size <= 0) {
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
        List<FieldSortBuilder> sorts = Lists.newArrayListWithCapacity(1);
        sorts.add(sort);
        return new BucketTopkPipelineAggregator(name, bucketsPaths, metaData, from, size, baseKeyName, sorts);
    }

    @Override
    public void doValidate(AggregatorFactory parent, Collection<AggregationBuilder> aggFactories,
                           Collection<PipelineAggregationBuilder> pipelineAggregatoractories) {
        if (sort == null && size == null && from == 0) {
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
//    @Override
//    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
//        List<FieldSortBuilder> sorts = Lists.newArrayListWithCapacity(1);
//        sorts.add(sort);
//        builder.field(FROM.getPreferredName(), from);
//        if (size != null) {
//            builder.field(SIZE.getPreferredName(), size);
//        }
//        builder.field(BASE_KEY_NAME.getPreferredName(), baseKeyName);
//        builder.field(SORT.getPreferredName(), sorts);
//        return builder;
//    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        List<FieldSortBuilder> sorts = Lists.newArrayListWithCapacity(1);
        sorts.add(sort);
        builder.field(FROM.getPreferredName(), from);
        if (size != null) {
            builder.field(SIZE.getPreferredName(), size);
        }
        builder.field(BASE_KEY_NAME.getPreferredName(), baseKeyName);
        builder.field(SORT.getPreferredName(), sorts);
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
        List<FieldSortBuilder> sorts = Lists.newArrayListWithCapacity(1);
        sorts.add(sort);
        return Objects.hash(super.hashCode(), sorts, from, size);
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
//                && Objects.equals(gapPolicy, other.gapPolicy);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
