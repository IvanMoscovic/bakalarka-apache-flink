package fi.muni.bp;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import fi.muni.bp.Enums.TopNOptions;
import fi.muni.bp.events.ConnectionEvent;
import fi.muni.bp.functions.AggregationsTumblingWindow;
import fi.muni.bp.functions.FoldAggregation;
import fi.muni.bp.source.MonitoringEventSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Peeve on 11.1.2017.
 */
@SuppressWarnings("ALL")
public class FoldAggregationsMain {

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
                        return measurementTime.getMillis();
                    }
                });

        FoldAggregation foldAggregation = new FoldAggregation(inputEventStream);
        foldAggregation.sumAggregateInTimeWin(TopNOptions.SRC_IP_ADDR, 2, 1, 10).print();

        env.execute("job");
    }

}
