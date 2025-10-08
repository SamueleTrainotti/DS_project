package com.example.ring;

/**
 * Represents a single key-value data item in the distributed store.
 *
 * <p>This class encapsulates the core data structure, including a key, a value, and a version number.
 * The version is used for simple conflict resolution or to track updates, although the current
 * implementation primarily just increments it.
 */
public class DataItem {
    private int version;
    private Long key;
    private String value;

    /**
     * Constructs a new DataItem.
     *
     * @param version The version of the data item.
     * @param key     The unique key identifying the data item.
     * @param value   The value associated with the key.
     */
    public DataItem(int version, Long key, String value) {
        this.version = version;
        this.key = key;
        this.value = value;
    }

    /**
     * Gets the version of the data item.
     *
     * @return The version number.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Sets the version of the data item.
     *
     * @param version The new version number.
     */
    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * Gets the key of the data item.
     *
     * @return The unique key.
     */
    public Long getKey() {
        return key;
    }

    /**
     * Sets the key of the data item.
     *
     * @param key The new unique key.
     */
    public void setKey(Long key) {
        this.key = key;
    }

    /**
     * Gets the value of the data item.
     *
     * @return The value.
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the value of the data item.
     *
     * @param value The new value.
     */
    public void setValue(String value) {
        this.value = value;
    }
}
