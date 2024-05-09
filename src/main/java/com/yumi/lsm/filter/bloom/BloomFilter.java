package com.yumi.lsm.filter.bloom;

import com.google.common.hash.Hashing;
import com.yumi.lsm.filter.BitsArray;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class BloomFilter {
    public static final Charset UTF_8 = StandardCharsets.UTF_8;
    /*用户指定数据*/
    //错误率
    private int f = 10;
    //预估存放数据量
    private int n = 128;
    /*计算数据*/
    //hash函数个数
    private int k;
    //布隆过滤器bit位数
    private int m;

    public static BloomFilter createByFn(int f, int n) {
        return new BloomFilter(f, n);
    }
    private BloomFilter(int f, int n) {
        if (f < 1 || f >= 100) {
            throw new IllegalArgumentException("f must be greater or equal than 1 and less than 100");
        }
        if (n < 1) {
            throw new IllegalArgumentException("n must be greater than 0");
        }
        this.f = f;
        this.n = n;
        double errorRate = f / 100.0;

        // set p =（1 - 1/m)^kn ~ e^(-kn/m) 错误标记为1的概率 （正确标记为1的概率为1/m）
        // errorRate = (1 - p)^k   k个位置全都被错误标记为1
        // when p = 0.5, k = (m/n) * ln2, errorRate = (1/2)^k
        // m >= n*log2(1/errorRate)*log2(e)
        this.k = (int) Math.ceil(logMN(0.5, errorRate));
        this.m = (int) Math.ceil(this.n * logMN(2, 1 / errorRate) * logMN(2, Math.E));
        //转换为8的整数倍（java中只有byte）
        this.m = (int) (Byte.SIZE * Math.ceil(this.m / (Byte.SIZE * 1.0)));
    }

    public int[] calcBitPositions(String str) {
        int[] bitPositions = new int[this.k];
        long hash64 = Hashing.murmur3_128().hashString(str, UTF_8).asLong();

        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= this.k; i++) {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitPositions[i - 1] = combinedHash % this.m;
        }
        return bitPositions;
    }

    public int[] calcBitPositions(byte[] bytes) {
        int[] bitPositions = new int[this.k];
        long hash64 = Hashing.murmur3_128().hashBytes(bytes).asLong();

        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= this.k; i++) {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitPositions[i - 1] = combinedHash % this.m;
        }
        return bitPositions;
    }

    public void hashTo(String str, BitsArray bits /*bit数为m的数组*/) {
        hashTo(calcBitPositions(str), bits);
    }

    public void hashTo(int[] bitPositions, BitsArray bits) {
        check(bits);
        for (int i : bitPositions) {
            bits.setBit(i, true);
        }
    }

    public boolean isHit(String str, BitsArray bits) {
        return isHit(calcBitPositions(str), bits);
    }

    public boolean isHit(int[] bitPositions, BitsArray bits) {
        check(bits);
        boolean ret = bits.getBit(bitPositions[0]);
        for (int i = 1; i < bitPositions.length; i++) {
            ret &= bits.getBit(bitPositions[i]);
        }
        return ret;
    }


    public boolean checkFalseHit(int[] bitPositions, BitsArray bits) {
        for (int pos : bitPositions) {
            if (!bits.getBit(pos)) {
                return false;
            }
        }
        return true;
    }

    public int getF() {
        return f;
    }

    public int getN() {
        return n;
    }

    public int getK() {
        return k;
    }

    public int getM() {
        return m;
    }

    protected void check(BitsArray bits) {
        if (bits.bitLength() != this.m) {
            throw new IllegalArgumentException(
                    String.format("Length(%d) of bits in BitsArray is not equal to %d!", bits.bitLength(), this.m)
            );
        }
    }

    private double logMN(double m, double n) {
        return Math.log(n) / Math.log(m);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BloomFilter))
            return false;

        BloomFilter that = (BloomFilter) o;

        if (f != that.f)
            return false;
        if (k != that.k)
            return false;
        if (m != that.m)
            return false;
        if (n != that.n)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = f;
        result = 31 * result + n;
        result = 31 * result + k;
        result = 31 * result + m;
        return result;
    }

    @Override
    public String toString() {
        return String.format("f: %d, n: %d, k: %d, m: %d", f, n, k, m);
    }
}
