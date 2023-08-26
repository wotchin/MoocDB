# SQL语法解析器
业内如果是C/C++实现的话，会套用bison, flex 这些库来完成语语法解析和词法解析过程。
由于编译器的前端语法解析和词法解析部分，涉及比较复杂的算法和数据结构，因此我们套用Python上的库:
- sly
- ply

关于sly的代码案例（计算器）：
> https://github.com/dabeaz/sly

# SQL引擎
## 逻辑算子
- ScanOperator: 用于扫描数据，是数据来源的最基本获取点
- GroupOperator: 用于做聚合
- HashOperator: 用户做连接操作
- SortOperator: 用于表明该SQL语句涉及排序操作
...
