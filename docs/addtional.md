# instrument
例如 MySQL的show查看数据库内部的一些信息，运行状态等等
SHOW

例如Postgres中，没有show语法，那么可以通过一些视图来查看数据库内部的一些运行状态


# ACL
用在安全领域
例如MySQL，通过表来限制访问用户的IP，用户名，... grant
Postgres则是通过 pg_hba.conf 来实现具体访问的IP地址

# 作业
要求：实现explain语句
explain是用来展示一个SQL语句的执行计划的
主要涉及到这几个技术点：
1. AST 这里面涉及到词法和语法解析部分
2. 也涉及到整体执行计划的生成
3. 大家要学会添加执行算子
