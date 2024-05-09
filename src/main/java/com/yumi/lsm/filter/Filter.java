package com.yumi.lsm.filter;

public interface Filter {
    //添加key到bloom过滤器中
    void add(byte[] key);
    //把缓存的BitArray导出
    BitsArray hash();
    //判断给定的key是否存在于给定的BitsArray
    boolean exist(byte[] key, BitsArray bitsArray);
    //重置 清空内置缓存BitsArray 以及 记录的key的数量
    void reset();
    //获取记录的key的数量
    //获取add操作
    int keyLen();
}
