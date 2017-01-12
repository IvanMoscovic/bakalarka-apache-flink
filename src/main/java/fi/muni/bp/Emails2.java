package fi.muni.bp;

import fi.muni.bp.events.EmailFromEvent;
import fi.muni.bp.events.EmailJoinEvent;
import fi.muni.bp.events.EmailToEvent;
import fi.muni.bp.functions.TimestampExtractor;
import fi.muni.bp.source.MonitoringEventSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * filter with base api
 * @author Ivan Moscovic  on 10.12.2016.
 */

@SuppressWarnings("ALL")
public class Emails2 {

    private static final String PATH0 = "C:/Users/Peeve/Desktop/nf";
    private static final String PATH = "C:/Users/Peeve/Desktop/data.nfjson";
    private static final String PATH2 = "C:/Users/Peeve/Desktop/testDoc2.txt";
    private static final String FromPATH = "C:/Users/Peeve/Desktop/ivan-sendmail/sendmail/from.json";
    private static final String ToPATH = "C:/Users/Peeve/Desktop/ivan-sendmail/sendmail/to.json";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        DataStream<EmailToEvent> inputEventStream1 = env
                .addSource(new MonitoringEventSource(EmailToEvent.class, ToPATH)).returns(EmailToEvent.class)
                .assignTimestampsAndWatermarks(new TimestampExtractor<EmailToEvent>());

        DataStream<EmailFromEvent> inputEventStream2 = env
                .addSource(new MonitoringEventSource(EmailFromEvent.class, FromPATH)).returns(EmailFromEvent.class)
                .assignTimestampsAndWatermarks(new TimestampExtractor<EmailFromEvent>());

        DataStream<EmailJoinEvent> connectedStreams =
                inputEventStream2.join(inputEventStream1)
                        .where((KeySelector<EmailFromEvent, Object>) emailFromEvent -> emailFromEvent.getQid())
                        .equalTo((KeySelector<EmailToEvent, Object>) emailToEvent -> emailToEvent.getQid())
                        .window(SlidingEventTimeWindows.of(Time.seconds(2),Time.seconds(1)))
                        .apply(new JoinFunction<EmailFromEvent, EmailToEvent, EmailJoinEvent>() {
                            @Override
                            public EmailJoinEvent join(EmailFromEvent emailFromEvent, EmailToEvent emailToEvent) throws Exception {
                                return createEmailJoinObject(emailFromEvent, emailToEvent);
                            }
                        })
                        .filter(new FilterFunction<EmailJoinEvent>() {
                            List<String> controls = new LinkedList<String>(Arrays.asList("gmail.com", "post.sk"));
                            @Override
                            public boolean filter(EmailJoinEvent emailJoinEvent) throws Exception {
                                if (!controls.contains(emailJoinEvent.getFrom_domain())){
                                    return false;
                                }
                                for(String domain : emailJoinEvent.getTo_domains()){
                                    if (!controls.contains(domain)){
                                        return true;
                                    }
                                }
                                return false;
                            }
                        });

        connectedStreams
                .map((MapFunction<EmailJoinEvent, String>) emailJoinEvent -> emailJoinEvent.getFrom_domain())
                .print();



        env.execute("CEP monitoring job");
    }

    private static EmailJoinEvent createEmailJoinObject(EmailFromEvent fromEvent, EmailToEvent toEvent){
        EmailJoinEvent joinEvent = new EmailJoinEvent();
        joinEvent.setDsn_1(toEvent.getDsn_1());
        joinEvent.setDsn_2(toEvent.getDsn_2());
        joinEvent.setDsn_3(toEvent.getDsn_3());
        joinEvent.setFrom(fromEvent.getFrom());
        joinEvent.setFrom_domain(fromEvent.getFrom_domain());
        joinEvent.setFromTimestamp(fromEvent.getTimestamp());
        joinEvent.setQid(fromEvent.getQid());
        joinEvent.setRelay_ip(fromEvent.getRelay_ip());
        joinEvent.setSendmail_uid(fromEvent.getSendmail_uid());
        joinEvent.setTo_domains(toEvent.getTo_domains());
        joinEvent.setStrTo_domains(toEvent.getTo_domains().toString());
        joinEvent.setToTimestamp(toEvent.getTimestamp());
        return joinEvent;
    }
}