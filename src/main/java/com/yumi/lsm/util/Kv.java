package com.yumi.lsm.util;

public class Kv {
    private final byte[] key;
    private final byte[] value;

    public Kv(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
