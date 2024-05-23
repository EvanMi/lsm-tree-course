package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.util.AllUtils;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public abstract class Block {

    //buffer 列表，写入的时候只写最后一个buffer，最后一个buffer写满以后，插入一个
    private List<ByteBuffer> record;
    //第一个key前的key为空
    private byte[] preKey = new byte[0];
    private int entriesCnt;
    private final Config config;

    public Block(Config config) {
        this.config = config;
        this.record = new ArrayList<>();
        this.record.add(this.config.getBlockBufferPool().borrowBuffer());
    }


    // 返回值表示是否写入成功
    public boolean append(byte[] key, byte[] value) {
        ByteBuffer buffer = this.record.get(this.record.size() - 1);
        int remainingBytes = buffer.limit() - buffer.position();
        int sharedKeyPrefixLen = AllUtils.sharedPrefixLen(key, this.preKey);

        int willWriteBytes = 4 //sharedKeyPrefixLen
                + 4 //selfKeySuffixLen
                + 4 //valueLen
                + key.length - sharedKeyPrefixLen //keySuffix
                + value.length; //value
        if (willWriteBytes > remainingBytes) {
            if (isFixedSize()) {
                //不能扩容，返回false
                return false;
            } else {
                buffer = this.config.getBlockBufferPool().borrowBuffer();
                this.record.add(buffer);
            }
        }

        //写入长度
        buffer.putInt(sharedKeyPrefixLen);
        buffer.putInt(key.length - sharedKeyPrefixLen);
        buffer.putInt(value.length);
        //写入内容
        buffer.put(key, sharedKeyPrefixLen, key.length - sharedKeyPrefixLen);
        buffer.put(value);

        this.preKey = key;
        this.entriesCnt++;
        return true;
    }

    protected abstract boolean isFixedSize();

    public int size() {
        return this.record.stream()
                .mapToInt(Buffer::position).sum();
    }

    public int flushTo(FileChannel fileChannel) {
        int size = this.size();
        try {
            for (ByteBuffer byteBuffer : this.record) {
                byteBuffer.flip();
                fileChannel.write(byteBuffer);
                this.config.getBlockBufferPool().returnBuffer(byteBuffer);
            }
            fileChannel.force(false);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            clear();
        }
        return size;
    }

    public void clear() {
        this.entriesCnt = 0;
        this.record.clear();
        this.preKey = new byte[0];
        this.record.add(this.config.getBlockBufferPool().borrowBuffer());
    }

    public byte[] getPreKey() {
        return preKey;
    }

    public int getEntriesCnt() {
        return entriesCnt;
    }

    public Config getConfig() {
        return config;
    }

    public List<ByteBuffer> getRecord() {
        return record;
    }
}
