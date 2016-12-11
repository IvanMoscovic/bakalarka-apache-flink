package fi.muni.bp.functions;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.joda.time.DateTime;

import java.lang.reflect.Field;

/**
 * @author Ivan Moscovic on 11.12.2016.
 */
public class TimestampExtractor<T> extends AscendingTimestampExtractor<T> {

    @Override
    public long extractAscendingTimestamp(T t) {
        Class<?> c = t.getClass();
        Field f = null;
        try {
            f = c.getDeclaredField("timestamp");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            throw new NullPointerException("your POJO class does not have needed attribute called timestamp");
        }
        f.setAccessible(true);
        try {
            return  ((DateTime) f.get(t)).getMillis();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
