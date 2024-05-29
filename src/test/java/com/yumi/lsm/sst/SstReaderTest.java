package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.BitsArray;
import com.yumi.lsm.filter.bloom.BloomFilter;
import com.yumi.lsm.util.AllUtils;
import com.yumi.lsm.util.Kv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class SstReaderTest {

    @Test
    public void testRead() {
        //文件设计

        // --- block 29
        // 内容 -> 0 1 1 byte[]{1} byte[]{1}  1 1 2 byte[]{2} byte[]{1,2}
        // 长度 -> 4 4 4 1         1          4 4 4 1         2  = 29
        // --- block 29
        // 内容 -> 0 1 1 byte[]{3} byte[]{3}  1 1 2 byte[]{3} byte[]{3,4}
        // 长度 -> 4 4 4 1         1          4 4 4 1         2  = 29

        // --- filter 365
        // BloomFilter.createByFn(20, 400).getM() == 1344b == 168B
        // 内容 -> 0 4 168 0 x?(168) 3 1 168 byte[]{29} y?(168)

        // --- index 44
        // 内容 -> 0 2 8 byte[]{1,2} 0 29 0 2 8 byte[]{3,4} 29 29

        // --- footer 16
        // 内容 -> 58 365 423 44

        byte[] key1 = new byte[] {1};
        byte[] value1 = new byte[] {1};
        byte[] key2 = new byte[] {1, 2};
        byte[] value2 = new byte[] {1, 2};
        byte[] key3 = new byte[] {3};
        byte[] value3 = new byte[] {3};
        byte[] key4 = new byte[] {3,4};
        byte[] value4 = new byte[] {3,4};

        byte[] expectedBytes = new byte[]
                /*block*/         {  0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 2, 1, 2,
                /*block*/            0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 3, 3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 4, 3, 4,
                /*filter*/           0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, -88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 68, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, -88, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                /*index*/            0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 1, 2, 0, 0, 0, 0, 0, 0, 0, 29,
                0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 3, 4, 0, 0, 0, 29, 0, 0, 0, 29,
                /*footer*/           0, 0, 0, 58, 0, 0, 1, 109, 0, 0, 1, -89, 0, 0, 0, 44 };
        String fileName = "yumi.sst";
        Config config = Config.newConfig("/tmp/yumi", c -> {
            c.setSstDataBlockSize(29);
        });

        File file = new File("/tmp/yumi/" + fileName);
        file.deleteOnExit();
        try {
            Files.write(file.toPath(), expectedBytes);
            SstReader sstReader = new SstReader(fileName, config);
            sstReader.readFooter();
            //footer信息和文件大小
            Assertions.assertEquals(58, sstReader.getFilterOffset());
            Assertions.assertEquals(365, sstReader.getFilterSize());
            Assertions.assertEquals(423, sstReader.getIndexOffset());
            Assertions.assertEquals(44, sstReader.getIndexSize());
            Assertions.assertEquals(expectedBytes.length, sstReader.size());
            //filter
            Map<Integer, BitsArray> blockToFilter = sstReader.readFilter();
            BloomFilter normalFilter = BloomFilter.createByFn(20, 400);
            Assertions.assertEquals(2, blockToFilter.size());
            BitsArray bitsArray0 = blockToFilter.get(0);
            Assertions.assertNotNull(bitsArray0);
            normalFilter.isHit(normalFilter.calcBitPositions(key1), bitsArray0);
            normalFilter.isHit(normalFilter.calcBitPositions(key2), bitsArray0);

            BitsArray bitsArray1 = blockToFilter.get(29);
            Assertions.assertNotNull(bitsArray1);
            normalFilter.isHit(normalFilter.calcBitPositions(key3), bitsArray1);
            normalFilter.isHit(normalFilter.calcBitPositions(key4), bitsArray1);
            //index
            Index[] indices = sstReader.readIndex();
            Assertions.assertEquals(2, indices.length);
            Index index0 = indices[0];
            Assertions.assertEquals(0, AllUtils.compare(key2, index0.getLastKey()));
            Assertions.assertEquals(0, index0.getBlockOffset());
            Assertions.assertEquals(29, index0.getBlockSize());

            Index index1 = indices[1];
            Assertions.assertEquals(0, AllUtils.compare(key4, index1.getLastKey()));
            Assertions.assertEquals(29, index1.getBlockOffset());
            Assertions.assertEquals(29, index1.getBlockSize());

            //readData
            Kv[] kvs1 = sstReader.readData();
            Assertions.assertEquals(4, kvs1.length);
            Assertions.assertEquals(0, AllUtils.compare(key3, kvs1[2].getKey()));
            Assertions.assertEquals(0, AllUtils.compare(value3, kvs1[2].getValue()));
            Assertions.assertEquals(0, AllUtils.compare(key4, kvs1[3].getValue()));
            Assertions.assertEquals(0, AllUtils.compare(value4, kvs1[3].getValue()));
            //readData(offset, size)
            Kv[] kvs2 = sstReader.readData(0, 29);
            Assertions.assertEquals(2, kvs2.length);
            Assertions.assertEquals(0, AllUtils.compare(key1, kvs2[0].getKey()));
            Assertions.assertEquals(0, AllUtils.compare(value1, kvs2[0].getValue()));
            Assertions.assertEquals(0, AllUtils.compare(key2, kvs2[1].getValue()));
            Assertions.assertEquals(0, AllUtils.compare(value2, kvs2[1].getValue()));


            sstReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
