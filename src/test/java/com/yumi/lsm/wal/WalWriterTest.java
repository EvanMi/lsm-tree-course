package com.yumi.lsm.wal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static com.yumi.lsm.wal.WalConstants.END_MARK;


public class WalWriterTest {

    @Test
    public void testCreateFailOfFull() {
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
        Assertions.assertThrowsExactly(IllegalStateException.class, () -> {
            new WalWriter(fileName, buffer.array().length);
        });
    }

    @Test
    public void testWriteNotFull() {
        String fileName = "/tmp/test.wal";
        File file = new File(fileName);
        file.deleteOnExit();
        int fileSize = 16;
        // file -> 4 + 4 + 1 + 1 4 0 0
        WalWriter walWriter = new WalWriter(fileName, fileSize);
        byte[] key = new byte[] {1};
        byte[] val = new byte[] {2};
        boolean res1 = walWriter.write(key, val);
        boolean res2 = walWriter.write(key, val);
        Assertions.assertTrue(res1);
        Assertions.assertFalse(res2);
        Assertions.assertEquals(fileSize, file.length());
        byte[] bytes = new byte[0];
        try {
            bytes = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            file.delete();
            throw new RuntimeException(e);
        }
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(1);
        buffer.putInt(1);
        buffer.put(key);
        buffer.put(val);
        buffer.putInt(END_MARK);
        byte[] expectedBytes = buffer.array();
        Assertions.assertEquals(expectedBytes.length, bytes.length);
        boolean compareRes = true;
        for (int i = 0; i < expectedBytes.length; i++) {
            compareRes &= bytes[i] == expectedBytes[i];
        }
        Assertions.assertTrue(compareRes);
        walWriter.close();
    }

    @Test
    public void testWriteFull() {
        String fileName = "/tmp/test.wal";
        File file = new File(fileName);
        file.deleteOnExit();

        int fileSize = 24;
        // file -> 4 + 4 + 1 + 1 4 + 4 + 1 + 1 4
        WalWriter walWriter = new WalWriter(fileName, fileSize);
        byte[] key = new byte[] {1};
        byte[] val = new byte[] {2};

        boolean res1 = walWriter.write(key, val);
        boolean res2 = walWriter.write(key, val);
        boolean res3 = walWriter.write(key, val);
        Assertions.assertTrue(res1 && res2);
        Assertions.assertFalse(res3);


        Assertions.assertEquals(fileSize, file.length());
        byte[] bytes = new byte[0];
        try {
            bytes = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            file.delete();
            throw new RuntimeException(e);
        }
        ByteBuffer buffer = ByteBuffer.allocate(24);
        buffer.putInt(1);
        buffer.putInt(1);
        buffer.put(key);
        buffer.put(val);
        buffer.putInt(1);
        buffer.putInt(1);
        buffer.put(key);
        buffer.put(val);
        buffer.putInt(END_MARK);
        byte[] expectedBytes = buffer.array();
        Assertions.assertEquals(expectedBytes.length, bytes.length);
        boolean compareRes = true;
        for (int i = 0; i < expectedBytes.length; i++) {
            compareRes &= bytes[i] == expectedBytes[i];
        }
        Assertions.assertTrue(compareRes);
        walWriter.close();
    }
}
