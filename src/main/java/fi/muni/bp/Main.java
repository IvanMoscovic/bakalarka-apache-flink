package fi.muni.bp;

import fi.muni.bp.events.ConnectionEvent;
import fi.muni.bp.functions.JSONMapperFromString;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.joda.time.DateTime;

/**
 * @author Ivan Moscovic on 14.11.2016.
 */
public class Main {
    private static final String PATH = "C:/Users/Peeve/Desktop/data.nfjson";
    private static final String PATH2 = "C:/Users/Peeve/Desktop/testDoc2.txt";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerType(DateTime.class);

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<ConnectionEvent> dataStream = env.readTextFile(PATH2)
                .map(new JSONMapperFromString<>(ConnectionEvent.class))
                .returns(ConnectionEvent.class)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ConnectionEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ConnectionEvent connection) {
                        return connection.getTimestamp().getMillis();
                    }
                });

        dataStream.print();

        env.execute("CEP monitoring job");
    }
}
