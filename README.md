# 这是一个基于java的LSM-TREE实现，供学习、参考使用；核心入口类为com.yumi.lsm.Tree
## tree核心函数
### put操作
### get操作
### remove操作（留作思考，待实现）
## 体验及性能表现
### 大量kv插入后进行查询
## 插入参考 com.yumi.lsm.Tree10000000LargeValueTest.testAdd
插入后的结果
![large-put-sst-result.png](imgs%2Flarge-put-sst-result.png)
![large-put-wal-result.png](imgs%2Flarge-put-wal-result.png)
### 基于JMH进行压测 详情参加类 com.yumi.lsm.benchmark.TreeBenchmark
#### 只进行插入的情况
- 代码内容如下
![tree-put-only.png](imgs%2Ftree-put-only.png)
- 压测结果
![tree-put-only-result.png](imgs%2Ftree-put-only-result.png)
#### 插入和查询混合
- 代码内容如下
![tree-get-put.png](imgs%2Ftree-get-put.png)
- 压测结果
![tree-get-put-result.png](imgs%2Ftree-get-put-result.png)
## 如何学习
代码的组织是按天来进行实现的，每天的代码获取可以在 https://github.com/EvanMi/lsm-tree-course/tags
配合每一天代码的视频可以在B站获取 https://www.bilibili.com/video/BV1tb421q7Ry/