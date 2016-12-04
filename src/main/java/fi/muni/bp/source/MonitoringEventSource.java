package fi.muni.bp.source;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.sun.xml.internal.bind.v2.TODO;
import fi.muni.bp.events.ConnectionEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.joda.time.DateTime;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * @author Ivan Moscovic on 26.11.2016.
 */
public class MonitoringEventSource<T> extends RichSourceFunction<T> {

    private volatile boolean isRunning = true;
    private ObjectMapper mapper = new ObjectMapper();
    private String filePath;
    private Class<T> type;
    private JavaType javaType;

    public MonitoringEventSource(Class<T> type, String filePath) {
        this.filePath = filePath;
        this.type = type;
    }

    @Override
    public void run(SourceFunction.SourceContext<T> sourceContext) throws Exception {

        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            Iterator<String> iterator = stream.iterator();
            while (isRunning && iterator.hasNext()) {
                T event = mapper.readValue(iterator.next(), javaType);
                sourceContext.collect(event);
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.javaType = mapper.getTypeFactory().constructType(type);
        mapper.registerModule(new JodaModule());
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}