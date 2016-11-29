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
import org.graphstream.graph.implementations.SingleGraph;
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

    public DataStream<Graph> createGraphs(String key, int timeWindowInSec, int topN){

        return this.dataStream.keyBy(key).window(TumblingEventTimeWindows.of(Time.seconds(timeWindowInSec))).
                apply(new WindowFunction<ConnectionEvent, Graph, Tuple, TimeWindow>() {

                    private List<Tuple3<String, Long, List<String>>> max = new LinkedList<>();
                    private String inWindow;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ConnectionEvent> iterable,
                                      Collector<Graph> collector) throws Exception {

                        List<String> listOfEdges = new LinkedList<String>();

                        String start = new Date(timeWindow.getStart()).toString();

                        if (start.equals(inWindow)){

                            Graph graph = new SingleGraph(start);
                            int count = 0;
                            for(Tuple3<String, Long, List<String>> elem : max) {
                                graph.addNode(elem.f0);
                                for (String to : elem.f2) {
                                    try {
                                        graph.addNode(to);
                                    } catch (IdAlreadyInUseException e) {
                                        //It's fine, node just already exists
                                    }
                                    try {
                                        graph.addEdge(String.valueOf(count), elem.f0, to, true);
                                        count++;
                                    } catch (Exception e) {
                                    }
                                }
                            }
                            collector.collect(graph);
                            max.clear();
                        }

                        String end = new Date(timeWindow.getEnd()).toString();
                        inWindow = end;

                        String from = iterable.iterator().next().getSrc_ip_addr();
                        String date = iterable.iterator().next().getTimestamp().toString();
                        Long sum = 0L;
                        for (ConnectionEvent t: iterable) {
                            sum += t.getBytes();
                            listOfEdges.add(t.getDst_ip_addr());
                        }
                        if (max.size() >= topN){
                            if (sum > max.get(max.size() - 1).f1){
                                max.remove(max.size() - 1);
                                max.add(Tuple3.of(from + " in window "+ start + " ", sum, listOfEdges));
                            }
                        }
                        else {
                            max.add(Tuple3.of(from + " in window "+ start + " " + end , sum, listOfEdges));
                        }
                        max.sort((Tuple3<String, Long, List<String>> o1, Tuple3<String, Long, List<String>> o2)
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
