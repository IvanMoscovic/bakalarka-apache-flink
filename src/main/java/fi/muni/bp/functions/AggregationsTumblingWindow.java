package fi.muni.bp.functions;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import fi.muni.bp.Enums.CardinalityOptions;
import fi.muni.bp.Enums.TopNOptions;
import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Ivan Moscovic on 22.11.2016.
 */
@SuppressWarnings("Duplicates")
public class AggregationsTumblingWindow {

    private DataStream<ConnectionEvent> dataStream;

    public AggregationsTumblingWindow(DataStream<ConnectionEvent> dataStream){
        this.dataStream = dataStream;
    }

    /**
     * Calculates topN IPaddr which transferred the most Bytes globally, updated after end of each window
     * @param key - src_ip or dst_ip
     * @param timeWindowInSec - window in seconds
     * @param topN - how many topN
     * @return - Tuple2 of (IPaddr + sum of transferred bytes)
     */
    public DataStream<List<Tuple3<DateTime ,String, Long>>> sumAggregateInTimeWin(TopNOptions key, int timeWindowInSec, int topN){

        return this.dataStream
                .keyBy(key.toString())
                .window(TumblingEventTimeWindows.of(Time.seconds(timeWindowInSec)))
                .apply(new WindowFunction<ConnectionEvent, List<Tuple3<DateTime ,String, Long>>, Tuple, TimeWindow>() {

                    private List<Tuple3<DateTime, String, Long>> max = new LinkedList<>();
                    private long end;
                    private StreamSummary<String> streamSummary = new StreamSummary<>(1000);

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<List<Tuple3<DateTime, String, Long>>> collector) throws Exception {

                        DateTime dateTime = new DateTime(timeWindow.getStart());
                        long start = dateTime.getMillis();

                        if (start == end){
                            for(Counter<String> t : streamSummary.topK(topN)){
                                max.add(Tuple3.of(dateTime, t.getItem(), t.getCount()));
                            }
                            collector.collect(max);
                            max.clear();
                        }

                        end = new DateTime(timeWindow.getEnd()).getMillis();

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
     * @param topN - number of results
     * @return Tuple3 of Date (start of the Window), actual value of cardOpt, count
     */
    public DataStream<Tuple3<DateTime, String, Long>> cardinality(CardinalityOptions cardOpt, int timeWindowInSec, int topN){

        return this.dataStream.keyBy(cardOpt.toString())
                .window(TumblingEventTimeWindows.of(Time.seconds(timeWindowInSec)))
                .apply(new WindowFunction<ConnectionEvent, Tuple3<DateTime, String, Long>, Tuple, TimeWindow>() {

                    private StreamSummary<String> streamSummary = new StreamSummary<>(1000);
                    private long end;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<Tuple3<DateTime, String, Long>> collector) throws Exception {

                        DateTime winTime = new DateTime(timeWindow.getStart());
                        long start = winTime.getMillis();

                        if (start == end){
                            for(Counter<String> t : streamSummary.topK(topN)){
                                if (t == null){
                                    break;
                                }
                                collector.collect(Tuple3.of(winTime, t.getItem(), t.getCount()));
                            }
                            streamSummary = new StreamSummary<>(1000);
                        }

                        end = winTime.getMillis();

                        Object o = iterable.iterator().next();
                        Class<?> c = o.getClass();
                        Field f = c.getDeclaredField(cardOpt.toString());
                        f.setAccessible(true);

                        for(ConnectionEvent event: iterable){
                            streamSummary.offer((String)(f.get(o)), 1);
                        }
                    }
                });

    }
}
