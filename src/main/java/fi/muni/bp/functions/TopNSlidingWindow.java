package fi.muni.bp.functions;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import fi.muni.bp.Enums.CardinalityOptions;
import fi.muni.bp.Enums.TopNOptions;
import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Ivan Moscovic on 3.12.2016.
 */
public class TopNSlidingWindow {

    private DataStream<ConnectionEvent> dataStream;

    public TopNSlidingWindow(DataStream<ConnectionEvent> dataStream){
        this.dataStream = dataStream;
    }

    /**
     * calculate topN
     * @param key src/dst ip address
     * @param timeWindowInSec - length of SlidingEventTimeWindows
     * @param slide - slide of the window
     * @param topN - number of results
     * @return List with topN Tuples<ip_addr, bytes sum>
     */
    public DataStream<List<Tuple2<String, Long>>> sumAggregateInTimeWin(TopNOptions key
            , int timeWindowInSec, int slide, int topN){

        return this.dataStream
                .keyBy(key.toString())
                .window(SlidingEventTimeWindows.of(Time.seconds(timeWindowInSec),Time.seconds(slide)))
                .apply(new WindowFunction<ConnectionEvent, List<Tuple2<String, Long>>, Tuple, TimeWindow>() {

                    private StreamSummary<String> streamSummary = new StreamSummary<>(1000);
                    private List<Tuple2<String, Long>> max = new LinkedList<>();
                    private long inWindow;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<List<Tuple2<String, Long>>> collector) throws Exception {

                        DateTime dateTime = new DateTime(timeWindow.getStart());
                        long start = dateTime.getMillis();

                        if (start == inWindow){
                            for(Counter<String> t : streamSummary.topK(topN)){
                                max.add(Tuple2.of(t.getItem() + " in window "+ dateTime + " ", t.getCount()));
                            }
                            collector.collect(max);
                            max.clear();
                        }

                        inWindow = dateTime.plusSeconds(slide).getMillis();

                        Object o = iterable.iterator().next();
                        Class<?> c = o.getClass();
                        Field f = c.getDeclaredField(key.toString());
                        f.setAccessible(true);

                        for(ConnectionEvent com : iterable){
                            streamSummary.offer((String) f.get(o),(int) com.getBytes());
                        }

                    }
                });
    }


    /**
     *
     * @param cardOpt - field for aggregation (enum possibilities src_port, dst_port, tos, protocol, tags
     * @param timeWindowInSec - length of window
     * @return Tuple3 of Date (start of the Window), actual value of cardOpt, count7
     * @param slide - slide of the window in sec
     * @param topN - number of result
     */
    public DataStream<Tuple3<DateTime, String, Long>> cardinality(CardinalityOptions cardOpt,
                                                              int timeWindowInSec, int slide, int topN){

        return this.dataStream.keyBy(cardOpt.toString())
                .window(SlidingEventTimeWindows.of(Time.seconds(timeWindowInSec), Time.seconds(slide)))
                .apply(new WindowFunction<ConnectionEvent, Tuple3<DateTime, String, Long>, Tuple, TimeWindow>() {

                    private StreamSummary<String> streamSummary = new StreamSummary<>(1000);
                    private long inWindow;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<Tuple3<DateTime, String, Long>> collector) throws Exception {

                        DateTime winTime = new DateTime(timeWindow.getStart());
                        long start = winTime.getMillis();

                        if (start == inWindow){
                            for(Counter<String> t : streamSummary.topK(topN)){
                                collector.collect(Tuple3.of(winTime, t.getItem(), t.getCount()));
                            }
                        }

                        inWindow = winTime.plusSeconds(slide).getMillis();

                        Object o = iterable.iterator().next();
                        Class<?> c = o.getClass();
                        Field f = c.getDeclaredField(cardOpt.toString());
                        f.setAccessible(true);

                        for(ConnectionEvent event: iterable){
                            streamSummary.offer(cardOpt.toString() + ": " + (String)(f.get(o)), 1);
                        }

                    }
                });

    }

}
