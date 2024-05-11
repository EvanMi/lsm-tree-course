package com.yumi.lsm.wal;

import com.yumi.lsm.memtable.MemTable;
import com.yumi.lsm.util.Kv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static com.yumi.lsm.wal.WalConstants.END_MARK;

public class WalReaderTest {

    @Test
    public void testLoadSuccess() {
        String fileName = "/tmp/test.wal";
        File file = new File(fileName);
        file.deleteOnExit();
        byte[] key = new byte[] {1};
        byte[] val = new byte[] {2};
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(1);
        buffer.putInt(1);
        buffer.put(key);
        buffer.put(val);
        buffer.putInt(END_MARK);
        try {
            Files.write(file.toPath(), buffer.array());
        } catch (IOException e) {
            file.deleteOnExit();
            throw new RuntimeException(e);
        }
        WalReader walReader = new WalReader(fileName);
        MemTable memTable = new MemTable() {
            private int size;
            @Override
            public void put(byte[] key, byte[] value) {
                this.size++;
            }

            @Override
            public Optional<byte[]> get(byte[] key) {
                return Optional.empty();
            }

            @Override
            public int size() {
                return size;
            }

            @Override
            public int entriesCnt() {
                return 0;
            }

            @Override
            public List<Kv> all() {
                return null;
            }
        };
        walReader.restoreMemTable(memTable);
        Assertions.assertEquals(1, memTable.size());
        walReader.close();
    }
}
