package fi.muni.bp.Enums;

/**
 * @author Ivan Moscovic on 3.12.2016.
 */
public enum TopNOptions {

    SRC_IP_ADDR("src_ip_addr"), DST_IP_ADDR("dst_ip_addr");

    private final String text;

    private TopNOptions(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
