package com.yumi.lsm.util;

public class AllUtils {
    private AllUtils() {}

    public static int compare(byte[] key1, byte[] key2) {
        if (key1 == key2) {
            return 0;
        }
        if (key1 == null || key2 == null) {
            return key1 == null ? -1 : 1;
        }
        int i = mismatch(key1, key2, Math.min(key1.length, key2.length));
        if (i >= 0) {
            return Byte.compare(key1[i], key2[i]);
        }
        return key1.length - key2.length;
    }

    private static int mismatch(byte[] key1, byte[] key2, int min) {
        for (int i = 0; i < min; i++) {
            if (key1[i] != key2[i]) {
                return i;
            }
        }
        return -1;
    }
}
