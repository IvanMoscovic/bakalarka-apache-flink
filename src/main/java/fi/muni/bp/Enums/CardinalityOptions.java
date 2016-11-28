package fi.muni.bp.Enums;

/**
 * @author Ivan Moscovic on 27.11.2016.
 */
public enum CardinalityOptions {
    PROTOCOL("protocol"),FLAGS("flags"), TOS("tos"), SRC_PORT("src_port"), DST_PORT("dst_port");

    private final String text;

    private CardinalityOptions(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
