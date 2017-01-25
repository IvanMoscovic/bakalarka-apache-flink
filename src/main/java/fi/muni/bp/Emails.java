package fi.muni.bp;

import fi.muni.bp.events.EmailFromEvent;
import fi.muni.bp.events.EmailJoinEvent;
import fi.muni.bp.events.EmailToEvent;
import fi.muni.bp.functions.TimestampExtractor;
import fi.muni.bp.source.MonitoringEventSource;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * filter uses table/sql api, not working so far
 * @author Ivan Moscovic  on 10.12.2016.
 */
@SuppressWarnings({"unchecked", "Duplicates"})
public class Emails {

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
                        });

        //connectedStreams.print();

        Table inputTable = tableEnv.fromDataStream(connectedStreams);
        tableEnv.registerFunction("myFilter", new MyFilter());
        String sqlStatement = sqlGenerator("muni.cz" ,"munipedie.cz", "munipedia.cz","mupedie.cz");
        Table resultTable = inputTable.filter(sqlStatement);
        //Table resultTable2 = resultTable.filter("myFilter(strTo_domains)");

        DataStream<EmailJoinEvent> joinEventDataStream = tableEnv.toDataStream(resultTable, EmailJoinEvent.class);
        joinEventDataStream.map((MapFunction<EmailJoinEvent, String>) emailJoinEvent -> emailJoinEvent.getFrom_domain() + " " + emailJoinEvent.getStrTo_domains())
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

    private static String sqlGenerator(String... domains){
        String sql = "";
        int i = 0;
        for (String domain : domains){
            i += 1;
            if (i == domains.length){
                sql += "from_domain = '" + domain + "\'";
                return sql;
            }
            sql += "from_domain = '" + domain + "' || ";
        }
        return null;
    }

    public static class MyFilter extends TableFunction<Boolean> {
        public void eval(String domains) {
            String s = domains.substring(1, domains.length()-1);
            List<String> domainsList = new LinkedList<String>(Arrays.asList(s.split(",")));
            List<String> control = Arrays.asList("muni.cz" ,"munipedie.cz", "munipedia.cz","mupedie.cz");
            for(String domain : domainsList) {
                if (!control.contains(domain)){
                    collect(true);
                    return;
                }
            }
            collect(false);
        }
    }
}