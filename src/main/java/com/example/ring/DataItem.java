package com.example.ring;

public class DataItem {
    private int version;
    private Long key;
    private String value;

    public DataItem(int version, Long key, String value) {
        this.version = version;
        this.key = key;
        this.value = value;
    }


    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Long getKey() {
        return key;
    }

    public void setKey(Long key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
