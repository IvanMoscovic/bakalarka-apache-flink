package fi.muni.bp.events;

import org.joda.time.DateTime;

import java.util.List;

/**
 * @author Ivan Moscovic on 10.1.2017.
 */
public class EmailJoinEvent {

    private String sendmail_uid;
    private String msgid;
    private String qid;
    private DateTime fromTimestamp;
    private DateTime toTimestamp;
    private String relay_ip;
    private String from;
    private String from_domain;
    private List<String> to_domains;
    private String strTo_domains;
    private int dsn_1;
    private int dsn_2;
    private int dsn_3;

    public EmailJoinEvent(String sendmail_uid, String msgid, String qid, DateTime fromTimestamp,
                          DateTime toTimestamp, String relay_ip, String from, String from_domain,
                          int dsn_1, List<String> to_domains, int dsn_3, int dsn_2, String strTo_domains) {
        this.sendmail_uid = sendmail_uid;
        this.qid = qid;
        this.fromTimestamp = fromTimestamp;
        this.toTimestamp = toTimestamp;
        this.relay_ip = relay_ip;
        this.from = from;
        this.from_domain = from_domain;
        this.dsn_1 = dsn_1;
        this.to_domains = to_domains;
        this.dsn_3 = dsn_3;
        this.dsn_2 = dsn_2;
        this.strTo_domains = strTo_domains;
    }

    public EmailJoinEvent() {
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getStrTo_domains() {
        return strTo_domains;
    }

    public void setStrTo_domains(String strTo_domains) {
        this.strTo_domains = strTo_domains;
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

    public DateTime getFromTimestamp() {
        return fromTimestamp;
    }

    public void setFromTimestamp(DateTime fromTimestamp) {
        this.fromTimestamp = fromTimestamp;
    }

    public DateTime getToTimestamp() {
        return toTimestamp;
    }

    public void setToTimestamp(DateTime toTimestamp) {
        this.toTimestamp = toTimestamp;
    }

    public String getRelay_ip() {
        return relay_ip;
    }

    public void setRelay_ip(String relay_ip) {
        this.relay_ip = relay_ip;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getFrom_domain() {
        return from_domain;
    }

    public void setFrom_domain(String from_domain) {
        this.from_domain = from_domain;
    }

    public List<String> getTo_domains() {
        return to_domains;
    }

    public void setTo_domains(List<String> to_domains) {
        this.to_domains = to_domains;
    }

    public int getDsn_1() {
        return dsn_1;
    }

    public void setDsn_1(int dsn_1) {
        this.dsn_1 = dsn_1;
    }

    public int getDsn_2() {
        return dsn_2;
    }

    public void setDsn_2(int dsn_2) {
        this.dsn_2 = dsn_2;
    }

    public int getDsn_3() {
        return dsn_3;
    }

    public void setDsn_3(int dsn_3) {
        this.dsn_3 = dsn_3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EmailJoinEvent joinEvent = (EmailJoinEvent) o;

        if (dsn_1 != joinEvent.dsn_1) return false;
        if (dsn_2 != joinEvent.dsn_2) return false;
        if (dsn_3 != joinEvent.dsn_3) return false;
        if (sendmail_uid != null ? !sendmail_uid.equals(joinEvent.sendmail_uid) : joinEvent.sendmail_uid != null)
            return false;
        if (msgid != null ? !msgid.equals(joinEvent.msgid) : joinEvent.msgid != null) return false;
        if (qid != null ? !qid.equals(joinEvent.qid) : joinEvent.qid != null) return false;
        if (fromTimestamp != null ? !fromTimestamp.equals(joinEvent.fromTimestamp) : joinEvent.fromTimestamp != null)
            return false;
        if (toTimestamp != null ? !toTimestamp.equals(joinEvent.toTimestamp) : joinEvent.toTimestamp != null)
            return false;
        if (relay_ip != null ? !relay_ip.equals(joinEvent.relay_ip) : joinEvent.relay_ip != null) return false;
        if (from != null ? !from.equals(joinEvent.from) : joinEvent.from != null) return false;
        if (from_domain != null ? !from_domain.equals(joinEvent.from_domain) : joinEvent.from_domain != null)
            return false;
        if (to_domains != null ? !to_domains.equals(joinEvent.to_domains) : joinEvent.to_domains != null) return false;
        return strTo_domains != null ? strTo_domains.equals(joinEvent.strTo_domains) : joinEvent.strTo_domains == null;

    }

    @Override
    public int hashCode() {
        int result = sendmail_uid != null ? sendmail_uid.hashCode() : 0;
        result = 31 * result + (msgid != null ? msgid.hashCode() : 0);
        result = 31 * result + (qid != null ? qid.hashCode() : 0);
        result = 31 * result + (fromTimestamp != null ? fromTimestamp.hashCode() : 0);
        result = 31 * result + (toTimestamp != null ? toTimestamp.hashCode() : 0);
        result = 31 * result + (relay_ip != null ? relay_ip.hashCode() : 0);
        result = 31 * result + (from != null ? from.hashCode() : 0);
        result = 31 * result + (from_domain != null ? from_domain.hashCode() : 0);
        result = 31 * result + (to_domains != null ? to_domains.hashCode() : 0);
        result = 31 * result + (strTo_domains != null ? strTo_domains.hashCode() : 0);
        result = 31 * result + dsn_1;
        result = 31 * result + dsn_2;
        result = 31 * result + dsn_3;
        return result;
    }
}
