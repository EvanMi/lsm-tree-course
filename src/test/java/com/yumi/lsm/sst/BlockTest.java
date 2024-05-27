package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class BlockTest {
    static Config config;
    static byte[] key1 = new byte[] {1};
    static byte[] val1 = new byte[] {1};
    static byte[] key2 = new byte[] {1, 2};
    static byte[] val2 = new byte[] {1, 2};

    @BeforeAll
    public static void init() {
        //block size
        // 内容 -> 0 1 1 byte[]{1} byte[]{1}  1 1 2 byte[]{2} byte[]{1,2}
        // 长度 -> 4 4 4 1         1          4 4 4 1         2  = 29
        config = Config.newConfig("/tmp", (c) -> {
            c.setBlockBufferPoolSize(4);
            c.setSstDataBlockSize(29);
        });
    }

    @AfterAll
    public static void destroy() {
        config.getBlockBufferPool().destroy(10);
    }


    @Test
    public void testDataBlockFull() {
        Block block = new DataBlock(config);
        boolean res1 = block.append(key1, val1);
        boolean res2 = block.append(key2, val2);
        Assertions.assertTrue(res1 && res2);
        boolean res3 = block.append(key1, val1);
        Assertions.assertFalse(res3);
    }

    @Test
    public void testExtendableBlockNeverFull() {
        Block block = new ExtendableBlock(config, 1024);
        boolean res1 = block.append(key1, val1);
        boolean res2 = block.append(key2, val2);
        boolean res3 = block.append(key1, val1);
        Assertions.assertTrue(res1 && res2 && res3);
        Assertions.assertEquals(key1, block.getPreKey());
        Assertions.assertEquals(3, block.getEntriesCnt());
        Assertions.assertEquals(2, block.getRecord().size());
    }

    @Test
    public void testFlushTo() {
        Block block = new ExtendableBlock(config, 1024);
        boolean res1 = block.append(key1, val1);
        boolean res2 = block.append(key2, val2);
        boolean res3 = block.append(key1, val1);
        Assertions.assertTrue(res1 && res2 && res3);
        Assertions.assertEquals(key1, block.getPreKey());
        Assertions.assertEquals(3, block.getEntriesCnt());
        Assertions.assertEquals(2, block.getRecord().size());
        int finalSize = block.size();

        File file = new File( "/tmp/ff.txt");
        file.deleteOnExit();
        try {
            RandomAccessFile rw = new RandomAccessFile(file, "rw");
            int res = block.flushTo(rw.getChannel());
            Assertions.assertEquals(res, finalSize);
            Assertions.assertEquals(0, block.getEntriesCnt());
            Assertions.assertEquals(1, block.getRecord().size());
            Assertions.assertEquals(0, block.size());
            rw.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

