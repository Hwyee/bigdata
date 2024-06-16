-- 192.168.1.111 中的新脚本。
-- 
-- 日期：2024年6月11日
-- 
-- 时间：上午12:04:54
set hive.auto.convert.join;
set hive.auto.convert.join.noconditionaltask;
set hive.mapjoin.smalltable.filesize;
set hive.auto.convert.join.nonconditionaltask.size;
set hive.auto.convert.join = false;
explain
select
	s.id,
	s.name
from
	student s
left outer join b on
	s.id = b.id
left outer join p on
	p.id = s.id;
--使用条件任务
set hive.auto.convert.join.noconditionaltask = false;
set hive.auto.convert.join.nonconditionaltask.size = 0;

--数据倾斜
select * from student s group by id;

--开启map-side group
set hive.map.aggr;

set hive.groupby.skewindata;

--join 倾斜,先mapreduce，再将倾斜的key进行map join。
set hive.optimize.skewjoin;
set hive.skewjoin.key;

