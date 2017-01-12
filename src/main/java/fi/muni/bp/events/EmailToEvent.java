package fi.muni.bp.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.List;

/**
 * @author Ivan Moscovic on 11.12.2016.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmailToEvent {

    private String entity;
    private String qid;
    private String relay_hostname;
    private List<String> to;
    private DateTime timestamp;
    private String relay_ip;
    private String mailer;
    private String delay;
    private String delays;
    private String orig_to;
    private String type;
    private String sendmail_uid;
    private int pri;
    private int dsn_1;
    private int dsn_2;
    private int dsn_3;
    private int relay_port;
    private String hostname;
    private String relay;
    private List<String> to_domains;
    private String xdelay;

    public EmailToEvent(){}

    public EmailToEvent(@JsonProperty("@entity") String entity,
                        @JsonProperty("qid") String qid,
                        @JsonProperty("dsn_2") int dsn_2,
                        @JsonProperty("relay_hostname") String relay_hostname,
                        @JsonProperty("orig_to") String orig_to,
                        @JsonProperty("delays") String delays,
                        @JsonProperty("to") List<String> to,
                        @JsonProperty("@timestamp") DateTime timestamp,
                        @JsonProperty("relay_ip") String relay_ip,
                        @JsonProperty("mailer") String mailer,
                        @JsonProperty("delay") String delay,
                        @JsonProperty("@type") String type,
                        @JsonProperty("sendmail_uid") String sendmail_uid,
                        @JsonProperty("pri") int pri,
                        @JsonProperty("dsn_1") int dsn_1,
                        @JsonProperty("dsn_3") int dsn_3,
                        @JsonProperty("relay_port") int relay_port,
                        @JsonProperty("relay") String relay,
                        @JsonProperty("Hostname") String hostname,
                        @JsonProperty("to_domains") List<String> to_domains,
                        @JsonProperty("xdelay") String xdelay) {
        this.entity = entity;
        this.qid = qid;
        this.dsn_2 = dsn_2;
        this.relay_hostname = relay_hostname;
        this.to = to;
        this.orig_to = orig_to;
        this.delays = delays;
        this.timestamp = timestamp;
        this.relay_ip = relay_ip;
        this.mailer = mailer;
        this.delay = delay;
        this.type = type;
        this.sendmail_uid = sendmail_uid;
        this.pri = pri;
        this.dsn_1 = dsn_1;
        this.dsn_3 = dsn_3;
        this.relay_port = relay_port;
        this.relay = relay;
        this.hostname = hostname;
        this.to_domains = to_domains;
        this.xdelay = xdelay;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public int getRelay_port() {
        return relay_port;
    }

    public void setRelay_port(int relay_port) {
        this.relay_port = relay_port;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getRelay() {
        return relay;
    }

    public void setRelay(String relay) {
        this.relay = relay;
    }

    public String getDelays() {
        return delays;
    }

    public String getOrig_to() {
        return orig_to;
    }

    public void setOrig_to(String orig_to) {
        this.orig_to = orig_to;
    }

    public void setDelays(String delays) {
        this.delays = delays;
    }

    public void setTimestamp(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public String getQid() {
        return qid;
    }

    public void setQid(String qid) {
        this.qid = qid;
    }

    public int getDsn_2() {
        return dsn_2;
    }

    public void setDsn_2(int dsn_2) {
        this.dsn_2 = dsn_2;
    }

    public String getRelay_hostname() {
        return relay_hostname;
    }

    public void setRelay_hostname(String relay_hostname) {
        this.relay_hostname = relay_hostname;
    }

    public List<String> getTo() {
        return to;
    }

    public void setTo(List<String> to) {
        this.to = to;
    }

    public String getRelay_ip() {
        return relay_ip;
    }

    public void setRelay_ip(String relay_ip) {
        this.relay_ip = relay_ip;
    }

    public String getMailer() {
        return mailer;
    }

    public void setMailer(String mailer) {
        this.mailer = mailer;
    }

    public String getDelay() {
        return delay;
    }

    public void setDelay(String delay) {
        this.delay = delay;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSendmail_uid() {
        return sendmail_uid;
    }

    public void setSendmail_uid(String sendmail_uid) {
        this.sendmail_uid = sendmail_uid;
    }

    public int getPri() {
        return pri;
    }

    public void setPri(int pri) {
        this.pri = pri;
    }

    public int getDsn_1() {
        return dsn_1;
    }

    public void setDsn_1(int dsn_1) {
        this.dsn_1 = dsn_1;
    }

    public int getDsn_3() {
        return dsn_3;
    }

    public void setDsn_3(int dsn_3) {
        this.dsn_3 = dsn_3;
    }

    public List<String> getTo_domains() {
        return to_domains;
    }

    public void setTo_domains(List<String> to_domains) {
        this.to_domains = to_domains;
    }

    public String getXdelay() {
        return xdelay;
    }

    public void setXdelay(String xdelay) {
        this.xdelay = xdelay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EmailToEvent that = (EmailToEvent) o;

        if (dsn_2 != that.dsn_2) return false;
        if (pri != that.pri) return false;
        if (dsn_1 != that.dsn_1) return false;
        if (dsn_3 != that.dsn_3) return false;
        if (relay_port != that.relay_port) return false;
        if (entity != null ? !entity.equals(that.entity) : that.entity != null) return false;
        if (qid != null ? !qid.equals(that.qid) : that.qid != null) return false;
        if (relay_hostname != null ? !relay_hostname.equals(that.relay_hostname) : that.relay_hostname != null)
            return false;
        if (to != null ? !to.equals(that.to) : that.to != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        if (relay_ip != null ? !relay_ip.equals(that.relay_ip) : that.relay_ip != null) return false;
        if (mailer != null ? !mailer.equals(that.mailer) : that.mailer != null) return false;
        if (delay != null ? !delay.equals(that.delay) : that.delay != null) return false;
        if (delays != null ? !delays.equals(that.delays) : that.delays != null) return false;
        if (orig_to != null ? !orig_to.equals(that.orig_to) : that.orig_to != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (sendmail_uid != null ? !sendmail_uid.equals(that.sendmail_uid) : that.sendmail_uid != null) return false;
        if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null) return false;
        if (relay != null ? !relay.equals(that.relay) : that.relay != null) return false;
        if (to_domains != null ? !to_domains.equals(that.to_domains) : that.to_domains != null) return false;
        return xdelay != null ? xdelay.equals(that.xdelay) : that.xdelay == null;

    }

    @Override
    public int hashCode() {
        int result = entity != null ? entity.hashCode() : 0;
        result = 31 * result + (qid != null ? qid.hashCode() : 0);
        result = 31 * result + dsn_2;
        result = 31 * result + (relay_hostname != null ? relay_hostname.hashCode() : 0);
        result = 31 * result + (to != null ? to.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (relay_ip != null ? relay_ip.hashCode() : 0);
        result = 31 * result + (mailer != null ? mailer.hashCode() : 0);
        result = 31 * result + (delay != null ? delay.hashCode() : 0);
        result = 31 * result + (delays != null ? delays.hashCode() : 0);
        result = 31 * result + (orig_to != null ? orig_to.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (sendmail_uid != null ? sendmail_uid.hashCode() : 0);
        result = 31 * result + pri;
        result = 31 * result + dsn_1;
        result = 31 * result + dsn_3;
        result = 31 * result + relay_port;
        result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
        result = 31 * result + (relay != null ? relay.hashCode() : 0);
        result = 31 * result + (to_domains != null ? to_domains.hashCode() : 0);
        result = 31 * result + (xdelay != null ? xdelay.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EmailFrom{" +
                "msgid='" + qid + '\'' +
                ", timestamp=" + timestamp +
                ", relay_ip=" + relay_ip +
                ", from_domain=" + to_domains +
                '}';
    }
}
