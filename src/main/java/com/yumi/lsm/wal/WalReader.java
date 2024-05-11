package com.yumi.lsm.wal;

import com.yumi.lsm.memtable.MemTable;
import com.yumi.lsm.util.BufferCleanUtil;
import com.yumi.lsm.util.Kv;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.yumi.lsm.wal.WalConstants.END_MARK;

public class WalReader {

    private final RandomAccessFile src;
    private final MappedByteBuffer reader;

    public WalReader(String file) {
        File srcFile = new File(file);
        if (!srcFile.exists()) {
            throw new IllegalStateException("文件不存在: " + file);
        }
        try {
            this.src = new RandomAccessFile(file, "r");
            this.reader = src.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, this.src.length());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void restoreMemTable(MemTable memTable) {
        ByteBuffer view = this.reader.slice();
        view.position(0);
        view.limit(view.capacity());
        List<Kv> kvs = readAll(view);
        for (Kv kv : kvs) {
            memTable.put(kv.getKey(), kv.getValue());
        }
    }

    private List<Kv> readAll(ByteBuffer view) {
        List<Kv> res = new ArrayList<>();
        while (view.hasRemaining()) {
            int keyLenOrMark = view.getInt();
            if (keyLenOrMark == END_MARK) {
                break;
            }
            int valLen = view.getInt();
            if (keyLenOrMark == 0 && valLen == 0) {
                break;
            }
            byte[] key = new byte[keyLenOrMark];
            byte[] val = new byte[valLen];
            view.get(key);
            view.get(val);
            res.add(new Kv(key, val));
        }
        return res;
    }

    public void close() {
        BufferCleanUtil.clean(this.reader);
        try {
            this.src.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
