package fi.muni.bp;

import fi.muni.bp.Enums.CardinalityOptions;
import fi.muni.bp.events.ConnectionEvent;
import fi.muni.bp.ArangoUtilities.ArangoSinkFunction;
import fi.muni.bp.functions.TopNAggregation;
import fi.muni.bp.source.MonitoringEventSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;

import org.joda.time.DateTime;
/**
 * @author Ivan Moscovic on 29.11.2016.
 */
@SuppressWarnings("ALL")
public class MainArangoConnection {

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

        agg.cardinality(CardinalityOptions.SRC_PORT, 10)
                .addSink(new ArangoSinkFunction("stats", CardinalityOptions.SRC_PORT));

        agg.cardinality(CardinalityOptions.SRC_PORT, 10).print();

        env.execute("CEP monitoring job");
    }

}
