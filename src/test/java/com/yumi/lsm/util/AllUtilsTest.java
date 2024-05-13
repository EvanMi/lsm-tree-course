package com.yumi.lsm.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AllUtilsTest {

    @Test
    public void testCompareSame() {
        byte[] key1Key2 = new byte[] {1, 2, 3, 4};
        int res = AllUtils.compare(key1Key2, key1Key2);
        Assertions.assertEquals(0, res);
    }

    @Test
    public void testCompareNull() {
        byte[] key1OrKey2 = new byte[0];
        int res1 = AllUtils.compare(null, key1OrKey2);
        int res2 = AllUtils.compare(key1OrKey2, null);
        Assertions.assertEquals(-1, res1);
        Assertions.assertEquals(1, res2);
    }

    @Test
    public void testCompareValueEquals() {
        byte[] key1 = new byte[]{1,2,3};
        byte[] key2 = new byte[]{1,2,3};
        int res = AllUtils.compare(key1, key2);
        Assertions.assertEquals(0, res);
    }

    @Test
    public void testCompareValueLess() {
        byte[] key1 = new byte[]{1,0,3};
        byte[] key2 = new byte[]{1,2,3};
        int res = AllUtils.compare(key1, key2);
        Assertions.assertEquals(-2, res);
    }

    @Test
    public void testCompareValueMore() {
        byte[] key1 = new byte[]{1,2,3};
        byte[] key2 = new byte[]{1,0,3};
        int res = AllUtils.compare(key1, key2);
        Assertions.assertEquals(2, res);
    }

    @Test
    public void testCompareLengthLess() {
        byte[] key1 = new byte[]{1,2,3};
        byte[] key2 = new byte[]{1,2,3,4};
        int res = AllUtils.compare(key1, key2);
        Assertions.assertEquals(-1, res);
    }

    @Test
    public void testCompareLengthMore() {
        byte[] key1 = new byte[]{1,2,3,4,5};
        byte[] key2 = new byte[]{1,2,3};
        int res = AllUtils.compare(key1, key2);
        Assertions.assertEquals(2, res);
    }

}
