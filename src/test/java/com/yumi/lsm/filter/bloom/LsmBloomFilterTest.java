package com.yumi.lsm.filter.bloom;

import com.yumi.lsm.filter.BitsArray;
import com.yumi.lsm.filter.LsmBloomFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LsmBloomFilterTest {

    @Test
    public void testAddSuccess() {
        LsmBloomFilter lsmBloomFilter = new LsmBloomFilter();
        byte[] key = {1, 2, 3};
        lsmBloomFilter.add(key);
        Assertions.assertEquals(1, lsmBloomFilter.keyLen());
    }


    @Test
    public void testExist() {
        LsmBloomFilter lsmBloomFilter = new LsmBloomFilter();
        byte[] key = {1, 2, 3};
        lsmBloomFilter.add(key);
        BitsArray hash = lsmBloomFilter.hash();
        boolean exist = lsmBloomFilter.exist(key, hash);
        Assertions.assertTrue(exist);
    }


    @Test
    public void testReset() {
        LsmBloomFilter lsmBloomFilter = new LsmBloomFilter();
        byte[] key = {1, 2, 3};
        lsmBloomFilter.add(key);
        BitsArray hash = lsmBloomFilter.hash();
        boolean exist = lsmBloomFilter.exist(key, hash);
        Assertions.assertTrue(exist);
        Assertions.assertEquals(1, lsmBloomFilter.keyLen());
        lsmBloomFilter.reset();
        BitsArray newHash = lsmBloomFilter.hash();
        boolean newExist = lsmBloomFilter.exist(key, newHash);
        Assertions.assertFalse(newExist);
        Assertions.assertEquals(0, lsmBloomFilter.keyLen());
    }
}
