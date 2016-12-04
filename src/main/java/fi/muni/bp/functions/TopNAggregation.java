package fi.muni.bp.functions;

import fi.muni.bp.Enums.CardinalityOptions;
import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Ivan Moscovic on 22.11.2016.
 */
@SuppressWarnings("Duplicates")
public class TopNAggregation {

    private DataStream<ConnectionEvent> dataStream;

    public TopNAggregation(DataStream<ConnectionEvent> dataStream){
        this.dataStream = dataStream;
    }

    /**
     * Calculates topN IPaddr which transferred the most Bytes in time window
     * @param key - src_ip or dst_ip
     * @param timeWindowInSec - window in seconds
     * @param topN - how many topN
     * @return - Tuple2 of (IPaddr + sum of transferred bytes)
     */
    public DataStream<List<Tuple2<String, Long>>> sumAggregateInTimeWin(String key, int timeWindowInSec, int topN){

        return this.dataStream
                .keyBy(key)
                .window(TumblingEventTimeWindows.of(Time.seconds(timeWindowInSec)))
                .apply(new WindowFunction<ConnectionEvent, List<Tuple2<String, Long>>, Tuple, TimeWindow>() {

                    private List<Tuple2<String, Long>> max = new LinkedList<>();
                    private String inWindow;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<List<Tuple2<String, Long>>> collector) throws Exception {

                        String start = new Date(timeWindow.getStart()).toString();

                        //collect result after each window
                        if (start.equals(inWindow)){
                            collector.collect(max);
                            max.clear();
                        }

                        String end = new Date(timeWindow.getEnd()).toString();
                        inWindow = end;

                        String from = iterable.iterator().next().getSrc_ip_addr();
                        String date = iterable.iterator().next().getTimestamp().toString();
                        Long sum = 0L;
                        for (ConnectionEvent t: iterable) {
                            sum += t.getBytes();
                        }
                        if (max.size() >= topN){
                            if (sum > max.get(max.size() - 1).f1){
                                max.remove(max.size() - 1);
                                max.add(Tuple2.of(from + " in window "+ start + " " + end, sum));
                            }
                        }
                        else {
                            max.add(Tuple2.of(from + " in window "+ start + " " + end , sum));
                        }
                        max.sort((Tuple2<String, Long> o1, Tuple2<String, Long> o2)-> o2.f1.compareTo(o1.f1));
                        //collector.collect(max);
                    }
                });
    }

    /**
     *
     * @param cardOpt - field for aggregation (enum possibilities src_port, dst_port, tos, protocol, tags
     * @param timeWindowInSec - length of window
     * @return Tuple3 of Date (start of the Window), actual value of cardOpt, count
     */
    public DataStream<Tuple3<Date, String, Long>> cardinality(CardinalityOptions cardOpt, int timeWindowInSec){

        return this.dataStream.keyBy(cardOpt.toString())
                .window(TumblingEventTimeWindows.of(Time.seconds(timeWindowInSec)))
                .apply(new WindowFunction<ConnectionEvent, Tuple3<Date, String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<Tuple3<Date, String, Long>> collector) throws Exception {

                        Date winTime = new Date(timeWindow.getStart());
                        long count = 0;
                        for(ConnectionEvent event: iterable){
                            count++;
                        }

                        Object o = iterable.iterator().next();
                        Class<?> c = o.getClass();
                        Field f = c.getDeclaredField(cardOpt.toString());
                        f.setAccessible(true);

                        String value;
                        try {
                            value = (String) f.get(o);
                        } catch (ClassCastException e){
                            throw new ClassCastException("Could not cast to String, you have use some illegal attribute");
                        }

                        collector.collect(Tuple3.of(winTime,value, count));
                    }
                });

    }
}
