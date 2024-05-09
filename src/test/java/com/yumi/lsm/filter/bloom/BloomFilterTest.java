package com.yumi.lsm.filter.bloom;

import com.yumi.lsm.filter.BitsArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BloomFilterTest {
    BloomFilter filter;
    BitsArray bitsArray;

    @BeforeEach
    public void init() {
        filter = BloomFilter.createByFn(10, 200);
        bitsArray = BitsArray.create(filter.getM());
    }

    @Test
    public void testAdd() {
        filter.hashTo("firstKey", bitsArray);
        filter.hashTo("secondKey", bitsArray);
        int[] firstKeyPositions = filter.calcBitPositions("firstKey");
        for (int firstKeyPosition : firstKeyPositions) {
            Assertions.assertTrue(bitsArray.getBit(firstKeyPosition));
        }
    }

    @Test
    public void testMatch() {
        filter.hashTo("firstKey", bitsArray);
        filter.hashTo("secondKey", bitsArray);
        Assertions.assertTrue(filter.isHit("firstKey", bitsArray));
        Assertions.assertTrue(filter.isHit("secondKey", bitsArray));
    }

    @Test
    public void testFakeMatch() {
        filter.hashTo("firstKey", bitsArray);
        filter.hashTo("secondKey", bitsArray);
        int[] firstInts = filter.calcBitPositions("firstKey");
        int[] secondInts = filter.calcBitPositions("secondKey");

        int n = firstInts.length;
        int[] fakePositions = new int[n];
        for (int i = 0; i < n; i++) {
            if (i < n / 2) {
                fakePositions[i] = firstInts[i];
            } else {
                fakePositions[i] = secondInts[i];
            }
        }
        Assertions.assertTrue(filter.checkFalseHit(fakePositions, bitsArray));
        Assertions.assertTrue(filter.isHit(fakePositions,bitsArray));
    }
}
