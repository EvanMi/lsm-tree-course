package com.yumi.lsm.memtable;
@FunctionalInterface
public interface MemTableConstructor {
    MemTable create();
}
