package fi.muni.bp.functions;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import fi.muni.bp.Enums.TopNOptions;
import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyWindowFunction;
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
 * @author Ivan Moscovic
 */
@SuppressWarnings("Duplicates")
public class FoldAggregation {
    private DataStream<ConnectionEvent> dataStream;

    public FoldAggregation(DataStream<ConnectionEvent> dataStream){
        this.dataStream = dataStream;
    }

    /**
     * calculate topN in window
     * @param key src/dst ip address
     * @param timeWindowInSec - length of SlidingEventTimeWindows
     * @param slide - slide of the window
     * @param topN - number of results
     * @return List with topN Tuples<ip_addr, bytes sum>
     */
    public DataStream<List<Tuple2<String, Long>>> sumAggregateInTimeWin(TopNOptions key
            , int timeWindowInSec, int slide, int topN) {

        return this.dataStream
                .keyBy(key.toString())
                .window(SlidingEventTimeWindows.of(Time.seconds(timeWindowInSec), Time.seconds(slide)))
                .fold(Tuple3.of(null, 0L, null), new FoldFunction<ConnectionEvent, Tuple3<String, Long, StreamSummary>>() {

                    private StreamSummary<String> streamSummary = new StreamSummary<>(1000);

                    @Override
                    public Tuple3<String, Long, StreamSummary> fold(Tuple3<String, Long, StreamSummary> stringLongStreamSummaryTuple3, ConnectionEvent o) throws Exception {
                        String ip = o.getSrc_ip_addr();
                        long bytes = stringLongStreamSummaryTuple3.f1 + o.getBytes();
                        streamSummary.offer(ip, Math.toIntExact(bytes));

                        return Tuple3.of(ip, bytes, streamSummary);
                    }
                })
                .flatMap(new FlatMapFunction<Tuple3<String, Long, StreamSummary>, List<Tuple2<String, Long>>>() {
                    @Override
                    public void flatMap(Tuple3<String, Long, StreamSummary> stringLongStreamSummaryTuple3, Collector<List<Tuple2<String, Long>>> collector) throws Exception {
                        List<Tuple2<String, Long>> tops = new LinkedList<>();
                        for (Object t : stringLongStreamSummaryTuple3.f2.topK(topN)) {
                            Counter counter = (Counter) t;
                            tops.add(Tuple2.of((String) counter.getItem(), counter.getCount()));
                        }
                        collector.collect(tops);
                    }
                });
    }

}
