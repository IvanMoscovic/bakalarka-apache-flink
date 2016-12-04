package fi.muni.bp.ArangoUtilities;

import com.arangodb.ArangoDriver;
import com.arangodb.entity.BaseDocument;
import fi.muni.bp.ArangoUtilities.BasicUtilities;
import fi.muni.bp.Enums.CardinalityOptions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Date;

/**
 * @author Ivan Moscovic on 29.11.2016.
 */
public class ArangoSinkFunction extends RichSinkFunction<Tuple3<Date, String, Long>> {

    private ArangoDriver arangoDriver;
    private String collection;
    private String field;

    public ArangoSinkFunction(String collection, CardinalityOptions cardinalityOptions){
        this.collection = collection;
        this.field = cardinalityOptions.toString();
    }

    public void open(Configuration configuration){
        arangoDriver = new ArangoDriver(BasicUtilities.setConfiguration("root", "root"));
        BasicUtilities basicUtilities = new BasicUtilities(arangoDriver);
        basicUtilities.createCollection(collection);
    }

    @Override
    public void invoke(Tuple3<Date, String, Long> tuple2) throws Exception {
        BaseDocument baseDocument = new BaseDocument();
        baseDocument.addAttribute("timeWindow", tuple2.f0);
        baseDocument.addAttribute(field, tuple2.f1);
        baseDocument.addAttribute("count", tuple2.f2);
        arangoDriver.createDocument(collection, baseDocument);

    }
}
