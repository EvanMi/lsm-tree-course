package com.yumi.lsm;

import com.yumi.lsm.filter.Filter;
import com.yumi.lsm.filter.LsmBloomFilter;
import com.yumi.lsm.memtable.MemTableConstructor;
import com.yumi.lsm.memtable.SkipListMemTable;
import com.yumi.lsm.sst.BlockBufferPool;

import java.io.File;
import java.util.function.Consumer;

public class Config {
    //工作目录
    private String dir;
    //最大的层级
    private int maxLevel = 7;
    //sst 文件基准大小 该值与 level 通过计算，决定某一个层中的sst文件的大小
    //默认值2MB
    private int sstSize = 2 * 1024 * 1024;
    //sst文件中 一个block的大小
    //默认值32KB
    private int sstDataBlockSize = 32 * 1024;
    //sst文件中 footer大小 固定为16B
    private int sstFooterSize = 16;
    //过滤器
    private Filter filter = new LsmBloomFilter();
    //memTable构造器
    private MemTableConstructor memTableConstructor = SkipListMemTable::new;
    //bufferPool
    private BlockBufferPool blockBufferPool;
    private int blockBufferPoolSize = 3000;

    private Config() {}

    public static Config newConfig(String dir, ConfigOption...options) {
        Config config = new Config();
        config.dir = dir;
        for (ConfigOption option : options) {
            option.accept(config);
        }
        //初始化bufferPool
        config.blockBufferPool = new BlockBufferPool(config);
        config.initAndCheck();
        return config;
    }

    private void initAndCheck() {
        File dirFile = new File(this.dir);
        if (!dirFile.exists()) {
            boolean makeDirRes = dirFile.mkdirs();
            if (!makeDirRes) {
                throw new IllegalStateException("创建 " + this.dir + " 失败");
            }
        }
        String walDir = dirFile + File.separator + "walfile";
        File walDirFile = new File(walDir);
        if (!walDirFile.exists()) {
            boolean makeDirRes = walDirFile.mkdirs();
            if (!makeDirRes) {
                throw new IllegalStateException("创建 " + walDir + "失败");
            }
        }
    }

    public String getDir() {
        return dir;
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    public int getSstSize() {
        return sstSize;
    }

    public int getLevelSstSize(int level) {
        if (level == 0) {
            return sstSize;
        }
        if (level == 1) {
            return sstSize * 10;
        }
        return sstSize * 100 * level;
    }

    public int getWalFileSize() {
        return sstSize * 4 / 5;
    }

    public int getSstDataBlockSize() {
        return sstDataBlockSize;
    }

    public int getSstFooterSize() {
        return sstFooterSize;
    }

    public Filter getFilter() {
        return filter;
    }

    public MemTableConstructor getMemTableConstructor() {
        return memTableConstructor;
    }

    public BlockBufferPool getBlockBufferPool() {
        return blockBufferPool;
    }

    public int getBlockBufferPoolSize() {
        return blockBufferPoolSize;
    }

    public void setMaxLevel(int maxLevel) {
        if (maxLevel <= 1) {
            throw new IllegalStateException("非法的maxLevel");
        }
        this.maxLevel = maxLevel;
    }

    public void setSstSize(int sstSize) {
        if (sstSize <= 0) {
            throw new IllegalStateException("非法的sstSize");
        }
        this.sstSize = sstSize;
    }

    public void setSstDataBlockSize(int sstDataBlockSize) {
        if (sstDataBlockSize <= 0) {
            throw new IllegalStateException("非法的sstDataBlockSize");
        }
        this.sstDataBlockSize = sstDataBlockSize;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public void setMemTableConstructor(MemTableConstructor memTableConstructor) {
        this.memTableConstructor = memTableConstructor;
    }

    public void setBlockBufferPool(BlockBufferPool blockBufferPool) {
        this.blockBufferPool = blockBufferPool;
    }

    public void setBlockBufferPoolSize(int blockBufferPoolSize) {
        if (blockBufferPoolSize <= 0) {
            throw new IllegalStateException("非法的blockBufferPoolSize");
        }
        this.blockBufferPoolSize = blockBufferPoolSize;
    }

    @FunctionalInterface
    public interface ConfigOption extends Consumer<Config> {}
}
