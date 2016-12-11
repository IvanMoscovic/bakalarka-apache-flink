package fi.muni.bp.ArangoUtilities;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.entity.*;
import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ivan Moscovic on 6.12.2016.
 */
public class GraphStreamToArangoGraphConverter extends RichSinkFunction<Graph> {

    private ArangoDriver arangoDriver;

    //constuctor for testing purposes
    /*public ArangoGraph() {
        ArangoConfigure configure = new ArangoConfigure();
        configure.setUser("root");
        configure.setPassword("root");
        configure.init();
        this.arangoDriver = new ArangoDriver(configure);
    }*/

    @Override
    public void open(Configuration configuration){
        ArangoConfigure configure = new ArangoConfigure();
        configure.setUser("root");
        configure.setPassword("root");
        configure.init();
        this.arangoDriver = new ArangoDriver(configure);
    }

    @Override
    public void invoke(Graph graph) throws ArangoException {

        //creating names for graph and collections from Graph id instance
        String vertexCollection = "IPaddr_" + graph.getId();
        String edgeCollection = "isConnected_" + graph.getId();
        String graphName = "IPGraph_" + graph.getId();
        int count = 0;

        //creating vertex collection for IPaddr and edge collection, registering edgeDefinition and creating graph
        arangoDriver.createCollection(edgeCollection, new CollectionOptions().setType(CollectionType.EDGE));
        arangoDriver.createCollection(vertexCollection, new CollectionOptions().setType(CollectionType.DOCUMENT));
        EdgeDefinitionEntity edgeDef = new EdgeDefinitionEntity();
        edgeDef.setCollection(edgeCollection);
        edgeDef.getFrom().add(vertexCollection);
        edgeDef.getTo().add(vertexCollection);
        List<EdgeDefinitionEntity> edgeDefinitions = new ArrayList<>();
        edgeDefinitions.add(edgeDef);
        arangoDriver.createGraph(graphName, edgeDefinitions, null, false);

        //starting of batch process
        arangoDriver.startBatchMode();

        BaseDocument src = new BaseDocument();
        BaseDocument target = new BaseDocument();

        for(Node from: graph.getEachNode()){
            try {
                src.setDocumentKey(from.getId());
                arangoDriver.graphCreateVertex(graphName, vertexCollection, src, false);
            } catch (ArangoException e) {
                //fine
            }
            for(Edge edge : from.getEachLeavingEdge()) {
                count++;
                try {
                    target.setDocumentKey(edge.getTargetNode().getId());
                    arangoDriver.graphCreateVertex(graphName, vertexCollection, target, false);
                } catch (ArangoException e) {
                    //fine
                }

                try {
                    ConnectionEvent connectionEvent = new ConnectionEvent();
                    connectionEvent.setBytes(edge.getAttribute("bytes"));
                    connectionEvent.setTimestamp(edge.getAttribute("timeStamp"));
                    connectionEvent.setProtocol(edge.getAttribute("protocol"));
                    connectionEvent.setSrc_port(edge.getAttribute("src_port"));
                    connectionEvent.setDst_port(edge.getAttribute("dst_port"));
                    arangoDriver.graphCreateEdge(graphName, edgeCollection,
                             vertexCollection+"/"+src.getDocumentKey(),
                             vertexCollection+"/"+target.getDocumentKey(), connectionEvent, false);
                } catch (ArangoException e) {
                    e.printStackTrace();
                }
                if (count == 125) {
                    arangoDriver.executeBatch();
                    arangoDriver.startBatchMode();
                    count = 0;
                }
            }
        }
        //commit rest, if there is any
        try {
            arangoDriver.executeBatch();
            arangoDriver.startBatchMode();
        } catch (ArangoException e){
            //fine, fails ony if there was nothing to commit (we want to recover from that)
        }
        //close the batch, so it can be started again with second graph over the same driver instance
        arangoDriver.cancelBatchMode();
        System.out.println("Graph " + graphName + " created in Arango");
    }




}

