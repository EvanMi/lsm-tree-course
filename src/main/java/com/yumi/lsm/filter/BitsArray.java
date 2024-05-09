package com.yumi.lsm.filter;

import java.util.Arrays;

public class BitsArray implements Cloneable {
    private final byte[] bytes;
    private final int bitLength;

    public static BitsArray create(int bitLength) {
        return new BitsArray(bitLength);
    }

    public static BitsArray create(byte[] bytes, int bitLength) {
        return new BitsArray(bytes, bitLength);
    }

    public static BitsArray create(byte[] bytes) {
        return new BitsArray(bytes);
    }

    private BitsArray(int bitLength) {
        this.bitLength = bitLength;
        // init bytes
        int temp = bitLength / Byte.SIZE;
        if (bitLength % Byte.SIZE > 0) {
            temp++;
        }
        this.bytes = new byte[temp];
        Arrays.fill(this.bytes, (byte) 0x00);
    }

    private BitsArray(byte[] bytes, int bitLength) {
        if (bytes == null || bytes.length < 1) {
            throw new IllegalArgumentException("Bytes is empty!");
        }

        if (bitLength < 1) {
            throw new IllegalArgumentException("Bit is less than 1.");
        }

        if (bitLength < bytes.length * Byte.SIZE) {
            throw new IllegalArgumentException("BitLength is less than bytes.length() * " + Byte.SIZE);
        }

        this.bytes = new byte[bytes.length];
        System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
        this.bitLength = bitLength;
    }

    private BitsArray(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            throw new IllegalArgumentException("Bytes is empty!");
        }

        this.bitLength = bytes.length * Byte.SIZE;
        this.bytes = new byte[bytes.length];
        System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
    }

    public int bitLength() {
        return this.bitLength;
    }

    public int byteLength() {
        return this.bytes.length;
    }

    public byte[] bytes() {
        return this.bytes;
    }

    public void xor(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
            this.bytes[i] = (byte) (this.bytes[i] ^ other.getByte(i));
        }
    }

    public void xor(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        boolean value = getBit(bitPos);
        setBit(bitPos, value ^ set);
    }

    public void or(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
            this.bytes[i] = (byte) (this.bytes[i] | other.getByte(i));
        }
    }

    public void or(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        if (set) {
            setBit(bitPos, true);
        }
    }

    public void and(final BitsArray other) {
        checkInitialized(this);
        checkInitialized(other);

        int minByteLength = Math.min(this.byteLength(), other.byteLength());

        for (int i = 0; i < minByteLength; i++) {
            this.bytes[i] = (byte) (this.bytes[i] & other.getByte(i));
        }
    }

    public void and(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);

        if (!set) {
            setBit(bitPos, false);
        }
    }

    public void not(int bitPos) {
        checkBitPosition(bitPos, this);

        setBit(bitPos, !getBit(bitPos));
    }

    public boolean getBit(int bitPos) {
        checkBitPosition(bitPos, this);
        return (this.bytes[subscript(bitPos)] & position(bitPos)) != 0;
    }

    public void setBit(int bitPos, boolean set) {
        checkBitPosition(bitPos, this);
        int sub = subscript(bitPos);
        int pos = position(bitPos);
        if (set) {
            this.bytes[sub] = (byte) (this.bytes[sub] | pos);
        } else {
            this.bytes[sub] = (byte) (this.bytes[sub] & ~pos);
        }
    }

    public byte getByte(int bytePos) {
        checkBytePosition(bytePos, this);
        return this.bytes[bytePos];
    }

    protected int subscript(int bitPos) {
        return bitPos / Byte.SIZE;
    }

    protected int position(int bitPos) {
        return 1 << bitPos % Byte.SIZE;
    }

    protected void checkInitialized(BitsArray bitsArray) {
        if (bitsArray.bytes() == null) {
            throw new RuntimeException("Not initialized!");
        }
    }

    protected void checkBytePosition(int bytePos, BitsArray bitsArray) {
        checkInitialized(bitsArray);
        if (bytePos > bitsArray.byteLength()) {
            throw new IllegalArgumentException("BytePos is greater than " + bytes.length);
        }
        if (bytePos < 0) {
            throw new IllegalArgumentException("BytePos is less than 0");
        }
    }

    protected void checkBitPosition(int bitPos, BitsArray bitsArray) {
        checkInitialized(bitsArray);
        if (bitPos > bitsArray.bitLength()) {
            throw new IllegalArgumentException("BitPos is greater than " + bitLength);
        }
        if (bitPos < 0) {
            throw new IllegalArgumentException("BitPos is less than 0");
        }
    }

    @Override
    public BitsArray clone() {
        byte[] clone = new byte[this.byteLength()];
        System.arraycopy(this.bytes, 0, clone, 0, this.byteLength());
        return create(clone, bitLength());
    }
}
