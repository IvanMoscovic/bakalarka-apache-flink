package fi.muni.bp.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ivan Moscovic on 28.11.2016.
 */
public class ElasticSearchSinkFunction implements ElasticsearchSinkFunction<Tuple2<String, Long>> {

    // construct index request
    @Override
    public void process(
            Tuple2<String, Long> record,
            RuntimeContext ctx,
            RequestIndexer indexer) {

        // construct JSON document to index
        Map<String, String> json = new HashMap<>();
        json.put("src_port", record.f0);
        json.put("count", String.valueOf(record.f1));

        IndexRequest rqst = Requests.indexRequest()
                .index("connection")           // index name
                .type("ports")     // mapping name
                .source(json);

        indexer.add(rqst);
    }
}
