package fi.muni.bp;

import fi.muni.bp.events.EmailFromEvent;
import fi.muni.bp.events.EmailToEvent;
import fi.muni.bp.functions.TimestampExtractor;
import fi.muni.bp.source.MonitoringEventSource;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Ivan Moscovic  on 10.12.2016.
 */
@SuppressWarnings("unchecked")
public class Emails {

    private static final String PATH0 = "C:/Users/Peeve/Desktop/nf";
    private static final String PATH = "C:/Users/Peeve/Desktop/data.nfjson";
    private static final String PATH2 = "C:/Users/Peeve/Desktop/testDoc2.txt";
    private static final String FromPATH = "C:/Users/Peeve/Desktop/ivan-sendmail/sendmail/from.json";
    private static final String ToPATH = "C:/Users/Peeve/Desktop/ivan-sendmail/sendmail/to.json";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        DataStream<EmailToEvent> inputEventStream = env
                .addSource(new MonitoringEventSource(EmailToEvent.class, ToPATH)).returns(EmailToEvent.class)
                .assignTimestampsAndWatermarks(new TimestampExtractor<EmailToEvent>());

        DataStream<EmailFromEvent> inputEventStream2 = env
                .addSource(new MonitoringEventSource(EmailFromEvent.class, FromPATH)).returns(EmailFromEvent.class)
                .assignTimestampsAndWatermarks(new TimestampExtractor<EmailFromEvent>());

        //inputEventStream.print();

        DataStream<Tuple2<String, String>> connectedStreams =
                inputEventStream2.join(inputEventStream)
                        .where((KeySelector<EmailFromEvent, Object>) emailFromEvent -> emailFromEvent.getQid())
                        .equalTo((KeySelector<EmailToEvent, Object>) emailToEvent -> emailToEvent.getQid())
                        .window(SlidingEventTimeWindows.of(Time.seconds(2),Time.seconds(1)))
                        .apply(new JoinFunction<EmailFromEvent, EmailToEvent, Tuple2<String, String>>() {
                            @Override
                            public Tuple2<String, String> join(EmailFromEvent emailFromEvent, EmailToEvent emailToEvent) throws Exception {
                                return Tuple2.of(emailFromEvent.getFrom(), emailToEvent.getQid());
                            }
                        });

        connectedStreams.print();


        env.execute("CEP monitoring job");
    }

}
