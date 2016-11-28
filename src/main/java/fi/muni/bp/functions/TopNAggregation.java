package fi.muni.bp.functions;

import fi.muni.bp.Enums.CardinalityOptions;
import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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

    public DataStream<List<Tuple2<String, Long>>> sumAggregateInTimeWin(String key, int timeWindowInSec, int topN){

        return this.dataStream.keyBy(key).window(TumblingEventTimeWindows.of(Time.seconds(timeWindowInSec))).
                apply(new WindowFunction<ConnectionEvent, List<Tuple2<String, Long>>, Tuple, TimeWindow>() {

                    private List<Tuple2<String, Long>> max = new LinkedList<>();
                    private String inWindow;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<List<Tuple2<String, Long>>> collector) throws Exception {

                        String start = new Date(timeWindow.getStart()).toString();

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
                                max.add(Tuple2.of(from + " in window "+ start + " ", sum));
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


    public DataStream<Tuple2<String, Long>> protocolCardinality(CardinalityOptions cardOpt, int timeWindowInSec){

        return this.dataStream.keyBy(cardOpt.toString()).window(TumblingEventTimeWindows.of(Time.seconds(timeWindowInSec))).
                apply(new WindowFunction<ConnectionEvent, Tuple2<String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<Tuple2<String, Long>> collector) throws Exception {

                        long count = 0;
                        for(ConnectionEvent event: iterable){
                            count++;
                        }
                        Object o = iterable.iterator().next();
                        Class<?> c = o.getClass();
                        Field f = c.getDeclaredField(cardOpt.toString());
                        f.setAccessible(true);

                        String value = null;
                        try {
                            value = (String) f.get(o);
                        } catch (ClassCastException e){
                            //it's fine
                        }
                        if (value == null){
                            Long val = (Long) f.get(o);
                            value = String.valueOf(val);
                        }

                        collector.collect(Tuple2.of(value, count));
                    }
                });

    }


}
