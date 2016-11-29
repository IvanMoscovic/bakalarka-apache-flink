package fi.muni.bp.functions;

import com.arangodb.ArangoDriver;
import com.arangodb.entity.BaseDocument;
import fi.muni.bp.ArangoUtilities.BasicUtilities;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author Ivan Moscovic on 29.11.2016.
 */
public class ArangoSinkFunction extends RichSinkFunction<Tuple2<String, Long>> {

    private ArangoDriver arangoDriver;
    private String collection;
    private String field;

    public ArangoSinkFunction(String collection, String field){
        this.collection = collection;
        this.field = field;
    }

    public void open(Configuration configuration){
        arangoDriver = new ArangoDriver(BasicUtilities.setConfiguration("root", "root"));
        BasicUtilities basicUtilities = new BasicUtilities(arangoDriver);
        basicUtilities.createCollection(collection);
    }

    @Override
    public void invoke(Tuple2<String, Long> tuple2) throws Exception {
        BaseDocument baseDocument = new BaseDocument();
        baseDocument.addAttribute(field, tuple2.f0);
        baseDocument.addAttribute("count", tuple2.f1);
        arangoDriver.createDocument(collection, baseDocument);

    }
}
