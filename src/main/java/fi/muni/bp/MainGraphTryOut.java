package fi.muni.bp;


import fi.muni.bp.ArangoUtilities.GraphStreamToArangoGraphConverter;
import fi.muni.bp.events.ConnectionEvent;
import fi.muni.bp.functions.Graphs;
import fi.muni.bp.source.MonitoringEventSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.graphstream.graph.Graph;
import org.joda.time.DateTime;


@SuppressWarnings("unchecked")
public class MainGraphTryOut {

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

        Graphs agg = new Graphs(inputEventStream);
        /*agg.generateGraphs("src_ip_addr", 1, 100).addSink(new SinkFunction<Graph>() {
            @Override
            public void invoke(Graph graph) throws Exception {
                System.out.println(graph.getId());
            }
        });*/
        agg.generateGraphs("src_ip_addr", 1, 100).addSink(new GraphStreamToArangoGraphConverter());

        env.execute("CEP monitoring job");
    }
}
