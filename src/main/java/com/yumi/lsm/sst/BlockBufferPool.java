package com.yumi.lsm.sst;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.yumi.lsm.Config;
import com.yumi.lsm.util.BufferCleanUtil;
import com.yumi.lsm.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BlockBufferPool {
    //延时执行清除任务
    private static final ScheduledExecutorService forceCleanService = Executors.newSingleThreadScheduledExecutor();
    //最多缓存多少个buffer
    private final int poolSize;
    //每个buffer的大小
    private final int bufferSize;
    private final Deque<ByteBuffer> availableBuffers;
    private final ConcurrentSkipListSet<ByteBuffer> borrowed;
    private final ReentrantReadWriteLock borrowLock;
    private volatile boolean active = true;

    public BlockBufferPool(Config config) {
        this.poolSize = config.getBlockBufferPoolSize();
        this.bufferSize = config.getSstDataBlockSize();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
        this.borrowed = new ConcurrentSkipListSet<>();
        this.borrowLock = new ReentrantReadWriteLock();
    }

    public void init() {
        borrowLock.writeLock().lock();
        try {
            for (int i = 0; i < poolSize; i++) {
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bufferSize);
                //锁定内存
                long address = ((DirectBuffer) byteBuffer).address();
                Pointer pointer = new Pointer(address);
                LibC.INSTANCE.mlock(pointer, new NativeLong(bufferSize));
                //放入队列
                availableBuffers.offerLast(byteBuffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            borrowLock.writeLock().unlock();
        }

    }

    public void destroy(int forceInterval) {
        if (!active) {
            //防重
            return;
        }
        forceInterval = Math.max(forceInterval, 5);
        borrowLock.writeLock().lock();
        active = false;
        try {
            for (ByteBuffer byteBuffer : availableBuffers) {
                releaseAndCleanBuffer(byteBuffer);
            }
            availableBuffers.clear();

            forceCleanService.schedule(() -> {
                BlockBufferPool.this.borrowLock.writeLock().lock();
                try {
                    for (ByteBuffer byteBuffer : BlockBufferPool.this.borrowed) {
                        BlockBufferPool.this.releaseAndCleanBuffer(byteBuffer);
                    }
                    BlockBufferPool.this.borrowed.clear();
                } finally {
                    BlockBufferPool.this.borrowLock.writeLock().unlock();
                }
            }, forceInterval, TimeUnit.SECONDS);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            borrowLock.writeLock().unlock();
        }
    }

    private void releaseAndCleanBuffer(ByteBuffer byteBuffer) {
        final long address = ((DirectBuffer) byteBuffer).address();
        Pointer pointer = new Pointer(address);
        LibC.INSTANCE.munlock(pointer, new NativeLong(this.bufferSize));
        BufferCleanUtil.clean(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        if (!active) {
            throw new IllegalStateException("buffer pool 已关闭");
        }
        borrowLock.readLock().lock();
        try {
            ByteBuffer byteBuffer = this.availableBuffers.pollFirst();
            if (null == byteBuffer) {
                //降级
                byteBuffer = ByteBuffer.allocate(bufferSize);
            } else {
                //记录池中的buffer
                borrowed.add(byteBuffer);
            }
            return byteBuffer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            borrowLock.readLock().unlock();
        }
    }

    public void returnBuffer(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            return;
        }
        borrowLock.readLock().lock();
        try {
            //蜕皮
            ByteBuffer viewed = BufferCleanUtil.viewed(buffer);
            if (borrowed.remove(viewed)) {
                viewed.position(0);
                viewed.limit(bufferSize);
                if (active) {
                    this.availableBuffers.offerLast(viewed);
                } else {
                    releaseAndCleanBuffer(viewed);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            borrowLock.readLock().unlock();
        }
    }
}
