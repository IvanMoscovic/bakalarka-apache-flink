package fi.muni.bp.functions;


import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import fi.muni.bp.events.EmailJoinEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

/**
 * @author Ivan Moscovic on 14.1.2017.
 */
@SuppressWarnings("Duplicates")
public class EmailFunctions {

    private DataStream<EmailJoinEvent> dataStream;

    public EmailFunctions(DataStream<EmailJoinEvent> dataStream){
        this.dataStream = dataStream;
    }

    /*public DataStream msgid_cardinality(int timeWindowInSec, int slide){

        return this.dataStream.keyBy("msgid")
                .window(SlidingEventTimeWindows.of(Time.seconds(timeWindowInSec), Time.seconds(slide)))
                .fold(Tuple2.of(null, 0L), new FoldFunction<EmailJoinEvent, Tuple2<String, Long>>() {

                    private HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(10);

                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> dateTimeLongTuple2, EmailJoinEvent o) throws Exception {
                        hyperLogLogPlus.offer(o.getMsgid());
                        Long card = hyperLogLogPlus.cardinality();
                        return Tuple2.of(o.getFromTimestamp().toString() + " ", card);
                    }
                });
    }*/

    public DataStream msgid_cardinality(int timeWindowInSec, int slide) {

        return this.dataStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(timeWindowInSec), Time.seconds(slide)))
                .apply(new AllWindowFunction<EmailJoinEvent, Tuple3<DateTime, String, Long>, TimeWindow>() {

                    private HyperLogLogPlus hyperlog = new HyperLogLogPlus(10);
                    private long inWindow;

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<EmailJoinEvent> iterable,
                                      Collector<Tuple3<DateTime, String, Long>> collector) throws Exception {

                        DateTime winTime = new DateTime(timeWindow.getStart());
                        long start = winTime.getMillis();

                        for(EmailJoinEvent event: iterable){
                            hyperlog.offer(event.getMsgid());
                        }

                        if (start == inWindow){
                            collector.collect(Tuple3.of(winTime, (new DateTime(inWindow)).toString(), hyperlog.cardinality()));
                            hyperlog = new HyperLogLogPlus(10);
                        }

                        inWindow = winTime.plusSeconds(slide).getMillis();

                    }
                });
    }

}
