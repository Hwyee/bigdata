-- 192.168.1.111 中的新脚本。
-- 
-- 日期：2024年6月9日
-- 
-- 时间：下午10:43:28
--分区表  指定的分区字段 相当于伪列，也可以查询，筛选等。一个分区就是一个目录。
create table p(
	id int,
	name string

)
 partitioned by (day string)
 row format delimited fields terminated by '\t';

insert into table p partition(day = '20240609') values(1,'a'),(2,'b');
insert into table p  values(1,'a','20240610'),(2,'b','20240610');
select * from p;

--查看表的分区
show partitions p;

--增加分区
alter table p add partition(day='20240611');

--修复分区
--msck repair table tableName [add/drop/sync partitions];

--分桶表
create table b(
	id int,
	name string

)
clustered by(id)
into 4 buckets
row format delimited fields terminated by '\t';

insert into table b  values(1,'a'),(2,'b'),(3,'b'),(4,'b'),(5,'b');

select * from b;

--orc
CREATE TABLE test_orc( 
	id bigint,
	name string
)
stored AS orc;

explain extended 
select rank() over(order by id),DENSE_RANK () over(order by id),ROW_NUMBER () over(order by id) from student s ;
