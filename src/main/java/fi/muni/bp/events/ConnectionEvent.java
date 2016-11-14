package fi.muni.bp.events;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

/**
 * @author Ivan Moscovic
 */
public class ConnectionEvent {

    private String dst_ip_addr;
    private String src_ip_addr;
    private long bytes;
    private DateTime timestamp;
    private String duration;
    private DateTime date_first_seen;
    private String src_port;
    private String dst_port;
    private long protocol;
    private String flags;
    private long tos;
    private long packets;
    private int hash;

    public ConnectionEvent() {
    }

    @JsonCreator
    public ConnectionEvent(@JsonProperty("dst_ip_addr") String dst_ip_addr,
                      @JsonProperty("src_ip_addr") String src_ip_addr,
                      @JsonProperty("bytes") long bytes,
                      @JsonProperty("timestamp") DateTime timestamp,
                      @JsonProperty("duration") String duration,
                      @JsonProperty("date_first_seen") DateTime date_first_seen,
                      @JsonProperty("dst_port") String dst_port,
                      @JsonProperty("src_port") String src_port,
                      @JsonProperty("protocol") long protocol,
                      @JsonProperty("flags") String flags,
                      @JsonProperty("tos") long tos,
                      @JsonProperty("packets") long packets) {
        this.hash = src_ip_addr.hashCode();
        this.dst_ip_addr = dst_ip_addr;
        this.src_ip_addr = src_ip_addr;
        this.bytes = bytes;
        this.timestamp = timestamp;
        this.duration = duration;
        this.date_first_seen = date_first_seen;
        this.dst_port = dst_port;
        this.src_port = src_port;
        this.protocol = protocol;
        this.flags = flags;
        this.tos = tos;
        this.packets = packets;
    }

    public int getHash() {
        return hash;
    }

    public void setHash(int hash) {
        this.hash = hash;
    }

    public String getDst_ip_addr() {
        return dst_ip_addr;
    }

    public void setDst_ip_addr(String dst_ip_addr) {
        this.dst_ip_addr = dst_ip_addr;
    }

    public String getSrc_ip_addr() {
        return src_ip_addr;
    }

    public void setSrc_ip_addr(String src_ip_addr) {
        this.src_ip_addr = src_ip_addr;
    }

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public DateTime getDate_first_seen() {
        return date_first_seen;
    }

    public void setDate_first_seen(DateTime date_first_seen) {
        this.date_first_seen = date_first_seen;
    }

    public String getSrc_port() {
        return src_port;
    }

    public void setSrc_port(String src_port) {
        this.src_port = src_port;
    }

    public String getDst_port() {
        return dst_port;
    }

    public void setDst_port(String dst_port) {
        this.dst_port = dst_port;
    }

    public long getProtocol() {
        return protocol;
    }

    public void setProtocol(long protocol) {
        this.protocol = protocol;
    }

    public String getFlags() {
        return flags;
    }

    public void setFlags(String flags) {
        this.flags = flags;
    }

    public long getTos() {
        return tos;
    }

    public void setTos(long tos) {
        this.tos = tos;
    }

    public long getPackets() {
        return packets;
    }

    public void setPackets(long packets) {
        this.packets = packets;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionEvent that = (ConnectionEvent) o;

        if (bytes != that.bytes) return false;
        if (protocol != that.protocol) return false;
        if (tos != that.tos) return false;
        if (packets != that.packets) return false;
        if (!dst_ip_addr.equals(that.dst_ip_addr)) return false;
        if (!src_ip_addr.equals(that.src_ip_addr)) return false;
        if (!timestamp.equals(that.timestamp)) return false;
        if (duration != null ? !duration.equals(that.duration) : that.duration != null) return false;
        if (date_first_seen != null ? !date_first_seen.equals(that.date_first_seen) : that.date_first_seen != null)
            return false;
        if (src_port != null ? !src_port.equals(that.src_port) : that.src_port != null) return false;
        if (dst_port != null ? !dst_port.equals(that.dst_port) : that.dst_port != null) return false;
        return flags != null ? flags.equals(that.flags) : that.flags == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + dst_ip_addr.hashCode();
        result = 31 * result + src_ip_addr.hashCode();
        result = 31 * result + (int) (bytes ^ (bytes >>> 32));
        result = 31 * result + timestamp.hashCode();
        result = 31 * result + (duration != null ? duration.hashCode() : 0);
        result = 31 * result + (date_first_seen != null ? date_first_seen.hashCode() : 0);
        result = 31 * result + (src_port != null ? src_port.hashCode() : 0);
        result = 31 * result + (dst_port != null ? dst_port.hashCode() : 0);
        result = 31 * result + (int) (protocol ^ (protocol >>> 32));
        result = 31 * result + (flags != null ? flags.hashCode() : 0);
        result = 31 * result + (int) (tos ^ (tos >>> 32));
        result = 31 * result + (int) (packets ^ (packets >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Connection{" +
                "src_ip_addr='" + src_ip_addr + '\'' +
                ", timestamp=" + timestamp +
                ", bytes=" + bytes +
                ", packets=" + packets +
                '}';
    }
}
