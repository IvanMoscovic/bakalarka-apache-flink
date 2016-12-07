package fi.muni.bp.ArangoUtilities;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.entity.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ivan Moscovic on 6.12.2016.
 */
public class ArangoGraph extends RichSinkFunction<Graph> implements Serializable {

    private ArangoDriver arangoDriver;

    /*public ArangoGraph() {
        /*ArangoConfigure configure = new ArangoConfigure();
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

        String vertexCollection = "IPaddr_" + graph.getId();
        String edgeCollection = "isConnected_" + graph.getId();
        String graphName = "IPGraph_" + graph.getId();
        int count = 0;

        arangoDriver.createCollection(edgeCollection, new CollectionOptions().setType(CollectionType.EDGE));
        arangoDriver.createCollection(vertexCollection, new CollectionOptions().setType(CollectionType.DOCUMENT));
        EdgeDefinitionEntity edgeDef = new EdgeDefinitionEntity();
        edgeDef.setCollection(edgeCollection);
        edgeDef.getFrom().add(vertexCollection);
        edgeDef.getTo().add(vertexCollection);
        List<EdgeDefinitionEntity> edgeDefinitions = new ArrayList<>();
        edgeDefinitions.add(edgeDef);
        arangoDriver.createGraph(graphName, edgeDefinitions, null, false);
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
                    arangoDriver.graphCreateEdge(graphName, edgeCollection,
                             vertexCollection+"/"+src.getDocumentKey(),
                             vertexCollection+"/"+target.getDocumentKey(), null, false);
                } catch (ArangoException e) {
                    e.printStackTrace();
                }
                if (count == 125) {
                    System.out.println("som tu");
                    arangoDriver.executeBatch();
                    arangoDriver.startBatchMode();
                    count = 0;
                }
            }
            arangoDriver.executeBatch();
            arangoDriver.startBatchMode();
        }
    }




}

