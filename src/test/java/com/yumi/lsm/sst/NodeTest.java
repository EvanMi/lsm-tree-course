package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.util.AllUtils;
import com.yumi.lsm.util.Kv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;

public class NodeTest {
    static String fileName = "yumi.sst";
    static Config config;
    static SstWriter.FinishRes res;

    @BeforeAll
    public static void init() throws Exception{
        config = Config.newConfig("/tmp/yumi", c -> {
            c.setSstDataBlockSize(29);
        });
        SstWriter sstWriter = new SstWriter(fileName, config);
        byte[] key1 = new byte[] {1};
        byte[] value1 = new byte[] {1};
        sstWriter.append(key1, value1);
        byte[] key2 = new byte[] {1, 2};
        byte[] value2 = new byte[] {1, 2};
        sstWriter.append(key2, value2);
        byte[] key3 = new byte[] {3};
        byte[] value3 = new byte[] {3};
        sstWriter.append(key3,value3);
        byte[] key4 = new byte[] {3,4};
        byte[] value4 = new byte[] {3,4};
        sstWriter.append(key4,value4);
        res = sstWriter.finish();
    }



    @Test
    public void testGet() {
        SstReader sstReader = new SstReader(fileName, config);
        Node node = new Node(config, fileName, sstReader, res.getSize(), res.getBlockToFilter(), res.getIndices());
        Optional<byte[]> bytes = node.get(new byte[]{1});
        Assertions.assertTrue(bytes.isPresent());
        Assertions.assertEquals(0, AllUtils.compare(bytes.get(), new byte[]{1}));
        Optional<byte[]> bytes1 = node.get(new byte[]{5});
        Assertions.assertFalse(bytes1.isPresent());
        node.close();
    }

    @Test
    public void testGetRange() {
        SstReader sstReader = new SstReader(fileName, config);
        Node node = new Node(config, fileName, sstReader, res.getSize(), res.getBlockToFilter(), res.getIndices());
        Kv[] kvs = node.getRange(0, 29);
        Assertions.assertEquals(2, kvs.length);
        Assertions.assertEquals(0, AllUtils.compare(kvs[0].getKey(), new byte[]{1}));
        Assertions.assertEquals(0, AllUtils.compare(kvs[1].getKey(), new byte[]{1,2}));
        node.close();
    }


    @Test
    public void testDestroy() {
        SstReader sstReader = new SstReader(fileName, config);
        Node node = new Node(config, fileName, sstReader, res.getSize(), res.getBlockToFilter(), res.getIndices());
        File beforeDestroy = new File(config.getDir() + File.separator + fileName);
        Assertions.assertTrue(beforeDestroy.exists());
        node.destroy();
        File afterDestroy = new File(config.getDir() + File.separator + fileName);
        Assertions.assertFalse(afterDestroy.exists());
    }
}
