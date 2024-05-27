package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.BitsArray;
import com.yumi.lsm.filter.bloom.BloomFilter;
import com.yumi.lsm.util.AllUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class SstWriterTest {


    @Test
    public void testWriteSuccess() throws IOException {
        Config config = Config.newConfig("/tmp/yumi", c -> {
            c.setSstDataBlockSize(29);
        });
        SstWriter sstWriter = new SstWriter("yumi.sst", config);
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
        SstWriter.FinishRes res = sstWriter.finish();

        Index[] indices = res.getIndices();
        Assertions.assertEquals(2, indices.length);
        Index index0 = indices[0];
        Assertions.assertEquals(0, AllUtils.compare(key2, index0.getLastKey()));
        Assertions.assertEquals(0, index0.getBlockOffset());
        Assertions.assertEquals(29, index0.getBlockSize());

        Index index1 = indices[1];
        Assertions.assertEquals(0, AllUtils.compare(key4, index1.getLastKey()));
        Assertions.assertEquals(29, index1.getBlockOffset());
        Assertions.assertEquals(29, index1.getBlockSize());


        BloomFilter normalFilter = BloomFilter.createByFn(20, 400);

        Map<Integer, BitsArray> blockToFilter = res.getBlockToFilter();
        Assertions.assertEquals(2, blockToFilter.size());
        BitsArray bitsArray0 = blockToFilter.get(0);
        Assertions.assertNotNull(bitsArray0);
        normalFilter.isHit(normalFilter.calcBitPositions(key1), bitsArray0);
        normalFilter.isHit(normalFilter.calcBitPositions(key2), bitsArray0);

        BitsArray bitsArray1 = blockToFilter.get(29);
        Assertions.assertNotNull(bitsArray1);
        normalFilter.isHit(normalFilter.calcBitPositions(key3), bitsArray1);
        normalFilter.isHit(normalFilter.calcBitPositions(key4), bitsArray1);


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

        byte[] expectedBytes = new byte[]
       /*block*/         {  0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 2, 1, 2,
       /*block*/            0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 3, 3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 4, 3, 4,
       /*filter*/           0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, -88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 68, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, -88, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
       /*index*/            0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 1, 2, 0, 0, 0, 0, 0, 0, 0, 29,
                            0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 3, 4, 0, 0, 0, 29, 0, 0, 0, 29,
       /*footer*/           0, 0, 0, 58, 0, 0, 1, 109, 0, 0, 1, -89, 0, 0, 0, 44 };

        sstWriter.close();

        File file = new File("/tmp/yumi/yumi.sst");
        file.deleteOnExit();

        byte[] bytes = Files.readAllBytes(file.toPath());
        Assertions.assertEquals(0, AllUtils.compare(expectedBytes, bytes));


        Assertions.assertEquals(0,  config.getFilter().keyLen());
    }
}
