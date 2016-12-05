package fi.muni.bp;

import fi.muni.bp.Enums.CardinalityOptions;
import fi.muni.bp.Enums.TopNOptions;
import fi.muni.bp.events.ConnectionEvent;
import fi.muni.bp.functions.AggregationsSlidingWindow;
import fi.muni.bp.source.MonitoringEventSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.joda.time.DateTime;

/**
 * @author Ivan Moscovic on 1.12.2016.
 */
@SuppressWarnings({"Duplicates", "unchecked"})
public class MainSlidingWindow {
    private static final String PATH0 = "C:/Users/Peeve/Desktop/nf";
    private static final String PATH = "C:/Users/Peeve/Desktop/data.nfjson";
    private static final String PATH2 = "C:/Users/Peeve/Desktop/testDoc2.txt";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        DataStream<ConnectionEvent> inputEventStream = env
                .addSource(new MonitoringEventSource(ConnectionEvent.class, PATH)).returns(ConnectionEvent.class)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ConnectionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ConnectionEvent connection) {
                        DateTime measurementTime = connection.getTimestamp();
                        return measurementTime.getMillis();}});

        AggregationsSlidingWindow topNSlidingWindow = new AggregationsSlidingWindow(inputEventStream);
        topNSlidingWindow.sumAggregateInTimeWin(TopNOptions.SRC_IP_ADDR, 2, 1, 10).print();
        //topNSlidingWindow.cardinality(CardinalityOptions.PROTOCOL, 2, 1, 5).print();

        env.execute("CEP monitoring job");
    }
}
