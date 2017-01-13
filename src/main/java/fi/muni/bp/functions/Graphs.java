package fi.muni.bp.functions;

import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.IdAlreadyInUseException;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.graph.implementations.SingleGraph;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Ivan Moscovic on 26.11.2016.
 */
@SuppressWarnings("Duplicates")
public class Graphs implements Serializable {

    private DataStream<ConnectionEvent> dataStream;

    public Graphs(DataStream<ConnectionEvent> dataStream){
        this.dataStream = dataStream;
    }

    public DataStream<Graph> generateGraphs(String key, int timeWindowInSec, int topN){

        return this.dataStream
                .keyBy(key).window(TumblingEventTimeWindows.of(Time.seconds(timeWindowInSec)))
                .apply(new WindowFunction<ConnectionEvent, Graph, Tuple, TimeWindow>() {

                    //Tuple3<srcIPaddr, bytes, dstIPaddrs>
                    private List<Tuple3<String, Long, List<ConnectionEvent>>> max = new LinkedList<>();
                    private long end;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<Graph> collector) throws Exception {

                        DateTime startDateTime = new DateTime(timeWindow.getStart());
                        long start = startDateTime.getMillis();
                        List<ConnectionEvent> listOfEdges = new LinkedList<>();


                        if (start == end){
                            String hour = String.valueOf(startDateTime.getHourOfDay());
                            String min = String.valueOf(startDateTime.getMinuteOfHour());
                            String sec = String.valueOf(startDateTime.getSecondOfMinute());
                            Graph graph = new MultiGraph(hour+'-'+min+'-'+sec);
                            int count = 0;
                            for(Tuple3<String, Long, List<ConnectionEvent>> elem : max) {
                                try {
                                    graph.addNode(elem.f0);
                                }catch (IdAlreadyInUseException e){
                                    //It's fine, node just already exists
                                }
                                for (ConnectionEvent to : elem.f2) {
                                    try {
                                        graph.addNode(to.getDst_ip_addr());
                                    } catch (IdAlreadyInUseException e) {
                                        //It's fine, node just already exists
                                    }
                                    try {
                                        Edge edge = graph.addEdge(String.valueOf(count), elem.f0, to.getDst_ip_addr(), true);
                                        edge.addAttribute("bytes",to.getBytes());
                                        edge.addAttribute("src_port", to.getSrc_port());
                                        edge.addAttribute("dst_port", to.getDst_port());
                                        edge.addAttribute("protocol", to.getProtocol());
                                        edge.addAttribute("timeStamp", to.getTimestamp());
                                        count++;
                                    } catch (Exception e) {
                                    }
                                }
                            }
                            collector.collect(graph);
                            max.clear();
                        }

                        end = new DateTime(timeWindow.getEnd()).getMillis();

                        String from = iterable.iterator().next().getSrc_ip_addr();
                        Long sum = 0L;
                        for (ConnectionEvent t: iterable) {
                            sum += t.getBytes();
                            listOfEdges.add(t);
                        }
                        if (max.size() >= topN){
                            if (sum > max.get(max.size() - 1).f1){
                                max.remove(max.size() - 1);
                                max.add(Tuple3.of(from, sum, listOfEdges));
                            }
                        }
                        else {
                            max.add(Tuple3.of(from, sum, listOfEdges));
                        }
                        max.sort((Tuple3<String, Long, List<ConnectionEvent>> o1, Tuple3<String, Long, List<ConnectionEvent>> o2)
                                -> o2.f1.compareTo(o1.f1));
                    }
                });
    }


    /**
     * create graph
     * @param graph graph instance
     * @param count count generator for edge ID
     * @param topN list of nodes and edges
     * @return full graph
     */
    private Graph createGraph(Graph graph, int count, List<Tuple3<String, Long, List<String>>> topN){

        for(Tuple3<String, Long, List<String>> tuple : topN) {
            graph.addNode(tuple.f0);
            for (String to : tuple.f2) {
                try {
                    graph.addNode(to);
                } catch (IdAlreadyInUseException e) {
                    //It's fine, node just already exists
                }
                try {
                    graph.addEdge(String.valueOf(count), tuple.f0, to);
                    count++;
                } catch (Exception e) {
                }
            }
        }
        return graph;
    }
}
