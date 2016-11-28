package fi.muni.bp;

import fi.muni.bp.Enums.CardinalityOptions;
import fi.muni.bp.events.ConnectionEvent;
import fi.muni.bp.functions.JSONMapperFromString;
import fi.muni.bp.functions.TopNAggregation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ivan Moscovic on 14.11.2016.
 */
public class Main {

    private static final String PATH0 = "file:///C:/Users/Peeve/Desktop/nf";
    private static final String PATH = "file:///C:/Users/Peeve/Desktop/data.nfjson";
    private static final String PATH2 = "file:///C:/Users/Peeve/Desktop/testDoc2.txt";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        DataStream<ConnectionEvent> dataStream = env.readTextFile(PATH)
                .map(new JSONMapperFromString<>(ConnectionEvent.class))
                .returns(ConnectionEvent.class)
                .assignTimestampsAndWatermarks
                        (new AscendingTimestampExtractor<ConnectionEvent>() {
                            @Override
                            public long extractAscendingTimestamp(ConnectionEvent connectionEvent) {
                                return connectionEvent.getTimestamp().getMillis();
                            }
                        });

        //DataStream<String> dataStream1 = env.readTextFile(PATH2).keyBy(0).sum(0);

        //List<List<Tuple2<String, Long>>> bla = new ArrayList<>();

        TopNAggregation agg = new TopNAggregation(dataStream);
        agg.sumAggregateInTimeWin("src_ip_addr", 1, 3);
        //agg.protocolCardinality(CardinalityOptions.SRC_PORT, 1);

        /*System.out.println(bla.size());
        for (List<Tuple2<String, Long>> k : bla){
            System.out.println(k);
        }*/
                //writeAsText("C:/Users/Peeve/Desktop/flink/abcd", FileSystem.WriteMode.OVERWRITE);

        //dataStream.print();

        //dataStream.print();
        env.execute("CEP monitoring job");

        /*System.out.println(bla.size());
        for (List<Tuple2<String, Long>> k : bla){
            System.out.println(k);
        }*/
    }
}
