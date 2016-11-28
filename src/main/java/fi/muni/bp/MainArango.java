package fi.muni.bp;

import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.entity.*;
import fi.muni.bp.ArangoUtilities.BasicUtilities;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ivan Moscovic on 28.11.2016.
 */
public class MainArango {

    public static void main(String[] args) throws ArangoException {


        ArangoDriver arangoDriver = new ArangoDriver(BasicUtilities.setConfiguration("root", "root"));
        BasicUtilities basicUtilities = new BasicUtilities(arangoDriver);

        basicUtilities.createGraphWithCollections();

    }
}
