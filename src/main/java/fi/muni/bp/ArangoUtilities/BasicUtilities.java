package fi.muni.bp.ArangoUtilities;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.entity.BooleanResultEntity;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.EdgeDefinitionEntity;
import com.arangodb.entity.GraphEntity;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ivan Moscovic on 28.11.2016.
 */
public class BasicUtilities {

        private ArangoDriver arangoDriver;

        public BasicUtilities(ArangoDriver arangoDriver){
            this.arangoDriver = arangoDriver;
        }

        public static ArangoConfigure setConfiguration(String user, String password){

            final ArangoConfigure arangoConfigure = new ArangoConfigure();
            arangoConfigure.setUser(user);
            arangoConfigure.setPassword(password);
            arangoConfigure.init();

            return arangoConfigure;
        }


        public void createDatabase(String name) {
            try {
                System.out.println(arangoDriver);
                BooleanResultEntity createDatabase = arangoDriver.createDatabase(name);
                Assert.assertNotNull(createDatabase);
                Assert.assertNotNull(createDatabase.getResult());
                Assert.assertTrue(createDatabase.getResult());
            } catch (ArangoException e) {
                System.out.println("Failed to create database " + name + "; " + e.getMessage());
            }

            arangoDriver.setDefaultDatabase(name);
        }

        public void deleteDatabase(String name) {
            try {
                System.out.println(arangoDriver);
                System.out.println(name);
                arangoDriver.deleteDatabase(name);
            } catch (ArangoException e) {
                System.out.println("Failed to delete database " + name + "; " + e.getMessage());
            }
        }

        public void createCollection(String name) {
            try {
                CollectionEntity createCollection = arangoDriver.createCollection(name);
                Assert.assertNotNull(createCollection);
                Assert.assertNotNull(createCollection.getName());
                Assert.assertEquals(name, createCollection.getName());
            } catch (ArangoException e) {
                System.out.println("create collection failed. " + e.getMessage());
            }
        }

        public void deleteCollection(String name){
            try {
                arangoDriver.deleteCollection(name);
            } catch (ArangoException e){
                System.out.println("delete collection failed. " + e.getMessage());
            }
        }

    /**
     * creates Graph with edgeDefinition collection called isConnected with skipIndex on timeStamp field
     * and vertexCollection called IPaddr
     * @return graph
     */
    public GraphEntity createGraphWithCollections(){

            List<EdgeDefinitionEntity> edgeDefinitions = new ArrayList<EdgeDefinitionEntity>();
            EdgeDefinitionEntity edgeDefIsConnected = new EdgeDefinitionEntity();

            edgeDefIsConnected.setCollection("isConnected");

            List<String> from = new ArrayList<String>();
            from.add("IPaddr");
            edgeDefIsConnected.setFrom(from);

            List<String> to = new ArrayList<String>();
            to.add("IPaddr");
            edgeDefIsConnected.setTo(to);

            edgeDefinitions.add(edgeDefIsConnected);

            GraphEntity graph = null;
            try {
                graph = arangoDriver.createGraph("IPgraph", edgeDefinitions, null, true);
            } catch (ArangoException e){
                e.getMessage();
            }

            try {
                arangoDriver.createSkipListIndex("isConnected", true,"timestamp");
            } catch (ArangoException e) {
                e.printStackTrace();
            }

            try {
                return arangoDriver.getGraph("IPgraph");
            } catch (ArangoException e) {
                e.printStackTrace();
            }
            return null;
        }
}

