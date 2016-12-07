package fi.muni.bp.ArangoUtilities;

import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.entity.EdgeDefinitionEntity;
import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.joda.time.DateTime;

import java.util.Arrays;

/**
 * @author Ivan Moscovic on 6.12.2016.
 */
public class ArangoGraph extends RichSinkFunction<Graph> {

    private ArangoDriver arangoDriver;
    private BasicUtilities basicUtilities;

    public ArangoGraph() {
        arangoDriver = new ArangoDriver(BasicUtilities.setConfiguration("root", "root"));
        basicUtilities = new BasicUtilities(arangoDriver);
    }

    public void open(Configuration configuration) {
        arangoDriver = new ArangoDriver(BasicUtilities.setConfiguration("root", "root"));
        basicUtilities = new BasicUtilities(arangoDriver);
    }

    @Override
    public void invoke(Graph graph) throws ArangoException {

        String vertexCollection = "IPaddr";
        String edgeCollection = "isConnected";
        int count = 0;

        //Fake object as attribute
        ConnectionEvent connectionEvent = new ConnectionEvent("fds", "dad", 54, DateTime.now(),
                "das", DateTime.now(),"dasda", "dsa", "dasd","dasd","dadsa", 545);

        arangoDriver.createGraph(graph.getId(), true);
        arangoDriver.graphCreateVertexCollection(graph.getId(), vertexCollection);
        EdgeDefinitionEntity edgeDefIsConnected = new EdgeDefinitionEntity();
        edgeDefIsConnected.setCollection(edgeCollection);
        edgeDefIsConnected.setFrom(Arrays.asList(vertexCollection));
        edgeDefIsConnected.setTo(Arrays.asList(vertexCollection));
        arangoDriver.graphCreateEdgeDefinition(graph.getId(), edgeDefIsConnected);
        //arangoDriver.startBatchMode();

        DocumentEntity<BaseDocument> src = null;
        DocumentEntity<BaseDocument> target = null;
        for(Node from: graph.getEachNode()){
            try {
                BaseDocument baseDocument = new BaseDocument();
                baseDocument.setDocumentKey(from.getId());
                src = arangoDriver.graphCreateVertex(graph.getId(), vertexCollection, baseDocument, true);
            } catch (ArangoException e) {
                src = arangoDriver.getDocument(vertexCollection, from.getId(), BaseDocument.class);
            }
            for(Edge edge : from.getEachLeavingEdge()){
                count++;
                try {
                    BaseDocument baseDocument1 = new BaseDocument();
                    baseDocument1.setDocumentKey(edge.getTargetNode().getId());
                    target = arangoDriver.graphCreateVertex(graph.getId(),
                            vertexCollection, baseDocument1, true);
                } catch (ArangoException e) {
                    target = arangoDriver.getDocument(vertexCollection, edge.getTargetNode().getId(), BaseDocument.class);
                }

                try {

                    arangoDriver.graphCreateEdge(graph.getId(), edgeCollection,
                            src.getDocumentHandle(), target.getDocumentHandle(), connectionEvent, true);
                } catch (ArangoException e) {
                    e.printStackTrace();
                }
                /*if (count == 125){
                    System.out.println("som tu");
                    arangoDriver.executeBatch();
                    arangoDriver.startBatchMode();
                    count = 0;
                }*/
            }
        }
        //arangoDriver.executeBatch();
    }

}
