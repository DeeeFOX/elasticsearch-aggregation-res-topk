package com.matrix.mars.elasticsearch;

import com.matrix.mars.elasticsearch.search.aggregations.bucket.BucketTopkPipelineAggregationBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

public class BucketTopkTests extends ESTestCase {


    public void testParser() throws Exception {
        // can create the factory with utf8 separator
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"from\":1, \"size\":3, \"baseKeyName\": \"ts\", \"sort\": [{\"yoyo\":{\"order\": \"desc\"}}]}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        BucketTopkPipelineAggregationBuilder builder = BucketTopkPipelineAggregationBuilder.parse("bucket_topk", stParser);
        assertNotNull(builder);
    };
}
