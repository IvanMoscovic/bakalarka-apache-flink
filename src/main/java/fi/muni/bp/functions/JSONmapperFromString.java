package fi.muni.bp.functions;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @author Ivan Moscovic on 8.11.2016.
 */
public class JSONMapperFromString<T> extends RichMapFunction<String, T> {

    private ObjectMapper mapper = new ObjectMapper();
    private Class<T> type;
    private JavaType javaType;

    public JSONMapperFromString(Class<T> type) {
        this.type = type;
    }

    @Override
    public T map(String s) throws Exception {
        return mapper.readValue(s, javaType);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.javaType = mapper.getTypeFactory().constructType(type);
        mapper.registerModule(new JodaModule());
    }
}

