package fi.muni.bp.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

/**
 * @author Ivan Moscovic on 10.12.2016.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmailFromEvent {

    private String bodytype;
    private String type;
    private String sendmail_uid;
    private String qid;
    private String entity;
    private DateTime timestamp;
    private String proto;
    private String relay_ip;
    private String relay;
    private String from;
    private String relay_hostname;
    private String msgid;
    private int size;
    private int class_;
    private int nrcpts;
    private String daemon;
    private String from_domain;

    public EmailFromEvent() {
    }

    @JsonCreator
    public EmailFromEvent(@JsonProperty("bodytype") String bodytype,
                           @JsonProperty("@type") String type,
                           @JsonProperty("sendmail_uid") String sendmail_uid,
                           @JsonProperty("qid") String qid,
                           @JsonProperty("@entity") String entity,
                           @JsonProperty("@timestamp") DateTime timestamp,
                           @JsonProperty("proto") String proto,
                           @JsonProperty("relay_ip") String relay_ip,
                           @JsonProperty("relay") String relay,
                           @JsonProperty("from") String from,
                           @JsonProperty("relay_hostname") String relay_hostname,
                           @JsonProperty("msgid") String msgid,
                           @JsonProperty("size") int size,
                           @JsonProperty("class") int class_,
                           @JsonProperty("nrcpts") int nrcpts,
                           @JsonProperty("daemon") String daemon,
                           @JsonProperty("from_domain") String from_domain) {
        this.bodytype = bodytype;
        this.type = type;
        this.sendmail_uid = sendmail_uid;
        this.qid = qid;
        this.entity = entity;
        this.timestamp = timestamp;
        this.proto = proto;
        this.from = from;
        this.relay_hostname = relay_hostname;
        this.msgid = msgid;
        this.size = size;
        this.class_ = class_;
        this.nrcpts = nrcpts;
        this.daemon = daemon;
        this.from_domain = from_domain;
        if (relay_ip == null){
            this.relay_ip = relay;
        }
        if (msgid == null){
            this.msgid = "email event without msgid";
        }
    }


    public String getBodytype() {
        return bodytype;
    }

    public void setBodytype(String bodytype) {
        this.bodytype = bodytype;
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

    public String getQid() {
        return qid;
    }

    public void setQid(String qid) {
        this.qid = qid;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getProto() {
        return proto;
    }

    public void setProto(String proto) {
        this.proto = proto;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getRelay_ip() {
        return relay_ip;
    }

    public void setRelay_ip(String relay_ip) {
        this.relay_ip = relay_ip;
    }

    public String getRelay_hostname() {
        return relay_hostname;
    }

    public void setRelay_hostname(String relay_hostname) {
        this.relay_hostname = relay_hostname;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getNrcpts() {
        return nrcpts;
    }

    public void setNrcpts(int nrcpts) {
        this.nrcpts = nrcpts;
    }

    public int getClass_() {
        return class_;
    }

    public void setClass_(int class_) {
        this.class_ = class_;
    }

    public String getDaemon() {
        return daemon;
    }

    public void setDaemon(String daemon) {
        this.daemon = daemon;
    }

    public String getFrom_domain() {
        return from_domain;
    }

    public void setFrom_domain(String from_domain) {
        this.from_domain = from_domain;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EmailFromEvent that = (EmailFromEvent) o;

        if (size != that.size) return false;
        if (class_ != that.class_) return false;
        if (nrcpts != that.nrcpts) return false;
        if (bodytype != null ? !bodytype.equals(that.bodytype) : that.bodytype != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (sendmail_uid != null ? !sendmail_uid.equals(that.sendmail_uid) : that.sendmail_uid != null) return false;
        if (qid != null ? !qid.equals(that.qid) : that.qid != null) return false;
        if (entity != null ? !entity.equals(that.entity) : that.entity != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        if (proto != null ? !proto.equals(that.proto) : that.proto != null) return false;
        if (relay_ip != null ? !relay_ip.equals(that.relay_ip) : that.relay_ip != null) return false;
        if (relay != null ? !relay.equals(that.relay) : that.relay != null) return false;
        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        if (relay_hostname != null ? !relay_hostname.equals(that.relay_hostname) : that.relay_hostname != null)
            return false;
        if (msgid != null ? !msgid.equals(that.msgid) : that.msgid != null) return false;
        if (daemon != null ? !daemon.equals(that.daemon) : that.daemon != null) return false;
        return from_domain != null ? from_domain.equals(that.from_domain) : that.from_domain == null;

    }

    @Override
    public int hashCode() {
        int result = bodytype != null ? bodytype.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (sendmail_uid != null ? sendmail_uid.hashCode() : 0);
        result = 31 * result + (qid != null ? qid.hashCode() : 0);
        result = 31 * result + (entity != null ? entity.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (proto != null ? proto.hashCode() : 0);
        result = 31 * result + (relay_ip != null ? relay_ip.hashCode() : 0);
        result = 31 * result + (relay != null ? relay.hashCode() : 0);
        result = 31 * result + (from != null ? from.hashCode() : 0);
        result = 31 * result + (relay_hostname != null ? relay_hostname.hashCode() : 0);
        result = 31 * result + (msgid != null ? msgid.hashCode() : 0);
        result = 31 * result + size;
        result = 31 * result + class_;
        result = 31 * result + nrcpts;
        result = 31 * result + (daemon != null ? daemon.hashCode() : 0);
        result = 31 * result + (from_domain != null ? from_domain.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EmailFrom{" +
                "msgid='" + qid + '\'' +
                ", timestamp=" + timestamp +
                ", relay_ip=" + relay_ip +
                ", from_domain=" + from_domain +
                '}';
    }
}
