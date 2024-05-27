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

    public static int sharedPrefixLen(byte[] key, byte[] preKey) {
        int i = 0;
        for(; i < key.length && i < preKey.length; i++) {
            if (key[i] != preKey[i]) {
                break;
            }
        }
        return i;
    }

    public static byte[] getSeparatorBetween(byte[] preKey, byte[] key) {
        if (preKey.length == 0) {
            byte[] separator = new byte[key.length];
            System.arraycopy(key, 0, separator, 0, key.length);
            separator[separator.length - 1] = (byte)(separator[separator.length - 1] - 1);
            return separator;
        }
        return preKey;
    }
}
