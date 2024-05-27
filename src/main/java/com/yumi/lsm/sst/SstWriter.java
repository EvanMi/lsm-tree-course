package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.BitsArray;
import com.yumi.lsm.filter.Filter;
import com.yumi.lsm.util.AllUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SstWriter {

    private final Config config;
    private final RandomAccessFile file;
    private final FileChannel channel;

    private Block dataBlock;
    private Block filterBlock;
    private Block indexBlock;

    private Map<Integer, BitsArray> blockToFilter;
    private List<Index> indexArr;

    private byte[] preKey;
    private int preBlockOffset;
    private int preBlockSize;


    //buffers
    ByteBuffer filterKeyBuffer;
    ByteBuffer indexValueBuffer;
    ByteBuffer footerBuffer;

    public SstWriter(String file, Config config) {
        //所有的sst文件放在指定目录下
        File dest = new File(config.getDir() + File.separator + file);
        this.config = config;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(dest, "rw");
            this.file = randomAccessFile;
            this.channel = randomAccessFile.getChannel();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //用来进行数据写入，大小固定，会在写入过程中循环使用
        this.dataBlock = new DataBlock(config);
        this.filterBlock  = new ExtendableBlock(config, 4 * 1024);
        this.indexBlock = new ExtendableBlock(config, 4 * 1024);

        this.blockToFilter = new HashMap<>();
        //index相关
        this.indexArr = new ArrayList<>();
        this.preKey = new byte[0];
        this.preBlockOffset = 0;
        this.preBlockSize = 0;

        //buffers
        this.filterKeyBuffer = ByteBuffer.allocateDirect(4);
        this.indexValueBuffer = ByteBuffer.allocateDirect(8);
        this.footerBuffer = ByteBuffer.allocateDirect(config.getSstFooterSize());
    }


    public void append(byte[] key, byte[] value) throws IOException {
        boolean res = this.dataBlock.append(key, value);
        if (!res) {
            //把block数据刷到channel中
            boolean hasData = this.refreshBlock();
            if (hasData) {
                //把索引数据插入到列表中
                this.insertIndex(key);
            }

            res = this.dataBlock.append(key, value);
            if (!res) {
                throw new RuntimeException("bug");
            }
        }
        this.config.getFilter().add(key);
        this.preKey = key;
    }

    public int size() {
        try {
            return (int)this.channel.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            this.channel.close();
            this.file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public FinishRes finish() throws IOException{
        boolean hasData = this.refreshBlock();
        if (hasData) {
            this.insertIndex(this.preKey);
        }

        //记录footer信息
        ByteBuffer byteBuffer = this.footerBuffer;
        byteBuffer.clear();

        FileChannel fileChannel = this.channel;

        try {
            int dataPosition = (int) fileChannel.position();
            //数据的结尾，是filter的开始
            byteBuffer.putInt(dataPosition);
            this.filterBlock.flushTo(fileChannel);
            int filterPosition = (int) fileChannel.position();
            //filter占用的字节数
            byteBuffer.putInt(filterPosition - dataPosition);
            this.indexBlock.flushTo(fileChannel);
            int indexPosition = (int) fileChannel.position();
            //filter的结尾，是index的开始
            byteBuffer.putInt(filterPosition);
            byteBuffer.putInt(indexPosition - filterPosition);
            byteBuffer.flip();

            channel.write(byteBuffer);
            channel.force(false);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new FinishRes((int)fileChannel.position(), this.blockToFilter,
                this.indexArr.toArray(new Index[0]));
    }


    private void insertIndex(byte[] key) {
        byte[] indexKey = AllUtils.getSeparatorBetween(this.preKey, key);
        ByteBuffer byteBuffer = this.indexValueBuffer;
        byteBuffer.clear();
        byteBuffer.putInt(this.preBlockOffset);
        byteBuffer.putInt(this.preBlockSize);
        byteBuffer.flip();

        byte[] indexValue = new byte[8];
        byteBuffer.get(indexValue);

        this.indexBlock.append(indexKey, indexValue);

        Index index = new Index();
        index.setLastKey(indexKey);
        index.setBlockOffset(this.preBlockOffset);
        index.setBlockSize(this.preBlockSize);
        this.indexArr.add(index);
    }

    private boolean refreshBlock() throws IOException {
        Filter filter = this.config.getFilter();
        if (filter.keyLen() == 0) {
            return false;
        }
        this.preBlockOffset = (int) this.channel.position();
        this.preBlockSize = this.dataBlock.flushTo(this.channel);

        BitsArray bitsArray = filter.hash();
        this.blockToFilter.put(this.preBlockOffset, bitsArray);

        ByteBuffer byteBuffer = this.filterKeyBuffer;
        byteBuffer.clear();
        byteBuffer.putInt(this.preBlockOffset);
        byteBuffer.flip();
        byte[] filterKeyBytes = new byte[4];
        byteBuffer.get(filterKeyBytes);

        this.filterBlock.append(filterKeyBytes, bitsArray.bytes());
        filter.reset();

        return true;
    }

    public static class FinishRes {
        private int size;
        Map<Integer, BitsArray> blockToFilter;
        private Index[] indices;

        public FinishRes(int size, Map<Integer, BitsArray> blockToFilter, Index[] indices) {
            this.size = size;
            this.blockToFilter = blockToFilter;
            this.indices = indices;
        }

        public int getSize() {
            return size;
        }

        public Map<Integer, BitsArray> getBlockToFilter() {
            return blockToFilter;
        }

        public Index[] getIndices() {
            return indices;
        }
    }
}
