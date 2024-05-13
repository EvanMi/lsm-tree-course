package com.yumi.lsm.memtable;

import com.yumi.lsm.util.Kv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class SkipListMemTableTest {

    @Test
    public void testAdd() {
        MemTable memTable = new SkipListMemTable();
        memTable.put(new byte[]{1,2,3}, new byte[]{3,2,1});
        Assertions.assertEquals(6, memTable.size());
        Assertions.assertEquals(1, memTable.entriesCnt());

        memTable.put(new byte[]{1,2,3}, new byte[]{6,6,6,6,6});
        Assertions.assertEquals(6 + 2, memTable.size());
        Assertions.assertEquals(1, memTable.entriesCnt());

        memTable.put(new byte[]{3,2,1}, new byte[]{8,8,8});
        Assertions.assertEquals(6 + 2 + 6, memTable.size());
        Assertions.assertEquals(2, memTable.entriesCnt());
    }


    @Test
    public void testGetNull() {
        MemTable memTable = new SkipListMemTable();
        memTable.put(new byte[]{1,2,3}, new byte[]{3,2,1});
        memTable.put(new byte[]{3,2,1}, new byte[]{8,8,8});

        Optional<byte[]> res = memTable.get(new byte[]{0, 1, 0});
        Assertions.assertFalse(res.isPresent());
    }

    @Test
    public void testGetSuccess() {
        MemTable memTable = new SkipListMemTable();
        memTable.put(new byte[]{1,2,3}, new byte[]{3,2,1});
        memTable.put(new byte[]{3,2,1}, new byte[]{8,8,8});

        Optional<byte[]> res = memTable.get(new byte[]{1,2,3});
        Assertions.assertTrue(res.isPresent());
    }

    @Test
    public void testAll() {
        MemTable memTable = new SkipListMemTable();
        byte[] key1 = new byte[]{1,2,3};
        byte[] key2 = new byte[]{3,2,1};

        byte[] val1 = new byte[]{3,2,1};
        byte[] val2 = new byte[]{8,8,8};
        memTable.put(key1, val1);
        memTable.put(key2, val2);

        List<Kv> all = memTable.all();
        Assertions.assertEquals(2, all.size());
        Kv kv0 = all.get(0);
        Assertions.assertEquals(key1, kv0.getKey());
        Assertions.assertEquals(val1, kv0.getValue());

        Kv kv1 = all.get(1);
        Assertions.assertEquals(key2, kv1.getKey());
        Assertions.assertEquals(val2, kv1.getValue());
    }
}
