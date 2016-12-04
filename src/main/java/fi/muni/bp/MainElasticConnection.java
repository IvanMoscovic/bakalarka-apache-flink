package fi.muni.bp;

import fi.muni.bp.Enums.CardinalityOptions;
import fi.muni.bp.events.ConnectionEvent;
import fi.muni.bp.ElasticUtilities.ElasticSearchSinkFunction;
import fi.muni.bp.source.MonitoringEventSource;
import fi.muni.bp.functions.TopNAggregation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.joda.time.DateTime;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Ivan Moscovic on 26.11.2016.
 */
@SuppressWarnings("unchecked")
public class MainElasticConnection {

    private static final String PATH0 = "C:/Users/Peeve/Desktop/nf";
    private static final String PATH = "C:/Users/Peeve/Desktop/data.nfjson";
    private static final String PATH2 = "C:/Users/Peeve/Desktop/testDoc2.txt";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        DataStream<ConnectionEvent> inputEventStream = env
                .addSource(new MonitoringEventSource(ConnectionEvent.class, PATH2)).returns(ConnectionEvent.class)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ConnectionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ConnectionEvent connection) {
                        DateTime measurementTime = connection.getTimestamp();
                        return measurementTime.getMillis();}});

        TopNAggregation agg = new TopNAggregation(inputEventStream);

        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "100");
        config.put("cluster.name", "elasticsearch");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

        agg.cardinality(CardinalityOptions.PROTOCOL, 10)
                .addSink(new ElasticsearchSink<>(config, transports, new ElasticSearchSinkFunction()));

        agg.sumAggregateInTimeWin("src_ip_addr", 10, 3).print();

        env.execute("CEP monitoring job");
    }
}
