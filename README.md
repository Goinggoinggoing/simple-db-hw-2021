详细笔记：https://goinggoinggoing.github.io/2023/06/20/simple-db/

基于java语言，实现一个简易事务支持的关系型数据库

- 实现基本的遍历、连接、聚合和删除等基本**操作算子**，以及基于直方图的查询优化
- 实现BufferPool缓存Page，且实现基于**LRU**的页面淘汰机制
- 实现**页面级**的共享锁、排他锁和锁升级，实现**可串行化**的并发策略
- 实现多种死锁检测算法：**Timeout**、**Wait-for Graph** 、 **Global Orderings**(wait-die)
- 基于UNDO日志实现**STEAL/NO FORCE**策略，提供更灵活的缓冲区管理
- 实现基本的WAL(Write-Ahead Logging)策略实现**事务回滚与恢复**


一共6个lab
- lab1 实现基本的数据结构
  tuple, page, tupleDesc, iterator等等

- lab2 实现scan iterator

  ​	基于scan iterator 来实现各种聚合函数，比如avg，count，sum，join等

- lab3 join 优化

  ​	建立一个优化模型， 按照主键，非主键，scan 表代价，直方图等进行成本估计，根据估计值来确定多表join的顺序

- lab 4 事务以及锁

  ​	这一章相对较难，要自己实现一个简单的读写锁，但是6.830中简化了，实现了page-level的锁，粒度比较粗，还有多种死锁的情况，test很给力，建议在写的时候一定要看清楚是哪个transaction 拿到了哪些page的哪些lock，而且这里的代码会影响到后面的lab 5、6，这里主要是按照两阶段锁协议并且no steal / force 的策略

  ​	代码中实现基于**Timeout**、**Wait-for Graph** 、 **Global Orderings**(wait-die)死锁检测算法

- lab 5 B+ 树索引（TODO）

  ​	实现B+树索引，插入、删除、修改，难点在于要把B+树结构以及这三种操作逻辑要捋清楚，还有父节点，子节点；叶子兄弟节点，非叶子节点的指针问题，以及一些边界条件。

- lab 6 实现基于 log的rollback 和 recover

  ​	lab中并没有真正存在undo log 和redo log，日志结构比较简单，只需要根据偏移处理即可，可以理解成是逻辑上的undo log 和 redo log。基于UNDO日志实现**STEAL/NO FORCE**策略，提供更灵活的缓冲区管理；实现基本的WAL(Write-Ahead Logging)策略实现**事务回滚与恢复**


TODO：lab5