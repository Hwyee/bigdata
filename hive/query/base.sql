-- 192.168.1.111 中的新脚本。
-- 
-- 日期：2024年6月7日
-- 
-- 时间：下午11:34:34
--1 行已获取 - 26s (0.024s 获取), 2024-06-07 23:38:05
select distinct name from teacher;

--yarn -> local 运行在一个节点
set mapreduce.framework.name = local;
--聚合函数
--count
SELECT count(*) FROM teacher; --统计行
select count(name) from teacher; --不统计null值

select sum(id) from student;
select avg(id) from student;
select max(id) from student;

--分组 分组查询列必须是分组字段和聚合函数
select * from student;
select count(*) from student group by name;

select * from student where id > 2;


show functions;

--order by 危险操作，只有一个reduce，全局排序。
set mapreduce.job.reduces=3;
select * from student  order by id ;
select * from student  sort by id ;