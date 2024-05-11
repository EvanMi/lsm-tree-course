package com.yumi.lsm.wal;

import com.yumi.lsm.util.BufferCleanUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.yumi.lsm.wal.WalConstants.END_MARK;
import static com.yumi.lsm.wal.WalConstants.END_MARK_BYTES;

public class WalWriter {

    private RandomAccessFile dest;
    private MappedByteBuffer writer;
    private int maxSize;
    private int curPosition;

    public WalWriter(String file, int maxSize) {
        File destFile = new File(file);
        this.maxSize = maxSize;
        boolean needRecover= destFile.exists();
        try {
            this.dest = new RandomAccessFile(destFile, "rw");
            this.writer = dest.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, maxSize);
            if (needRecover) {
                int curPosition = 0;
                ByteBuffer view = this.writer.slice();
                while (curPosition < view.limit()) {
                    int keyLenOrMark = view.getInt(curPosition);
                    if (keyLenOrMark == END_MARK) {
                        throw new IllegalStateException("file write done");
                    }
                    int valLen = view.getInt(curPosition + 4);
                    if (keyLenOrMark == 0 && valLen == 0) {
                        break;
                    }
                    curPosition = curPosition + 4 + 4 + keyLenOrMark + valLen;
                }
                this.curPosition = curPosition;
                this.writer.position(curPosition);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param key 要写入的key
     * @param value 要写入的value
     * @return 是否写入成功 如果文件放不下这返回false
     */
    public boolean write(byte[] key, byte[] value) {
        int willWriteBytes = 4 //用一个int记录key的长度 4字节
                + 4 //用一个int记录value的长度 4字节
                + key.length //存放key内容 所占用的字节数
                + value.length; //存放value内容 所占用的字节数
        if (this.curPosition + willWriteBytes + END_MARK_BYTES > this.maxSize) {
            this.writer.putInt(END_MARK);
            return false;
        }
        writer.putInt(key.length);
        writer.putInt(value.length);
        writer.put(key);
        writer.put(value);
        //每次都刷，不会丢数据，但是性能受影响
        writer.force();
        this.curPosition += willWriteBytes;
        return true;
    }

    public void close() {
        BufferCleanUtil.clean(this.writer);
        try {
            this.dest.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
