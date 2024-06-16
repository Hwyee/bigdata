-- 192.168.1.111 中的新脚本。
-- 
-- 日期：2024年6月8日
-- 
-- 时间：下午10:40:04
show functions like "nvl";
--nvl(value,default_value) - Returns default value if value is null else returns value
desc function nvl;
desc function extended nvl;
SELECT nvl(null,'bla') FROM student s  LIMIT 1;

--单行函数
select 1+1;
select round(1.155,2);--1.16
select substring("abcde",3,2);-- cd
select substring("abcde",-2,2);--de
select replace("aa","a","A"); --AA
select REGEXP_REPLACE('33aa10','\\d+','cc'); --ccaacc

select "aa" REGEXP '\\w'; --true
select repeat('a',3);--aaa
--split 第二个参数是正则表达式
select split('a,a,a',',');--{a,a,a}
select split('a.a.a','\\.');--{a,a,a}

select concat('a','b','c');
select 'a' || 'b';
-- 指定分隔符 with separator
select concat_ws('-','a','b','c');-- a-b-c
select concat_ws('-',array('a','b','c')); -- a-b-c

--json解析
desc function extended get_json_object;
select GET_JSON_OBJECT('{"name":"like","age":18}',"$"); 
select GET_JSON_OBJECT('{"name":"like","age":18}',"$.age"); 

--current_date date_add date_format date_sub
--datediff  to_date
show functions like "*date*";

select current_date;
desc function extended date_format;
select date_format(current_date,'y');

show functions like "*time*";
select current_timestamp();--2024-06-08 23:25:16.727
desc function extended unix_timestamp;
select unix_timestamp();
-- 按utc时区转换
select unix_timestamp(current_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS'); -- 这个只到秒

select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS'); --2024-06-08 15:34:00.000

select from_utc_timestamp(unix_timestamp()*1000,"GMT+8");--2024-06-08 23:40:28.000


select month(current_date);
select day(current_timestamp);

select datediff('2023-01-01','2024-01-01');--  -365
select date_add('2024-01-01',-1); -- 2023-12-31

-- case when then
select case when 1==2 then 'a' when 1==1 then 'b' else 'c' end as op; --b
select case 1 when 2 then 'a' when 1 then 'b' else 'c' end as op;
--if
select if(1==2,'b','a'); --a



show functions like "struct";
select struct('a','b',2);--[{"col1":"a","col2":"b","col3":2}]
select named_struct('a',1,'b','bb');--[{"a":1,"b":"bb"}]
DESC FUNCTION extended create_union;
--第一个参数相当于下标，从第二个元素开始获取，下标从0开始 
select create_union(0,1,'c');
select create_union(1,1,'c');

--聚合函数
select sum(if(id>1,10,0)) from student s ;
select count(if(id>100,null,id)) from student s ;
--返回集合 不去重
select COLLECT_LIST(id) from student s ;
--返回集合 去重
select COLLECT_SET(name) from teacher t ;

--UDTF 炸裂函数 table-generating functions 接收一行，输出一行或多行
desc function extended explode;
select explode(array('a','b','c')) as a; -- 多行
select explode(map(1,1,2,2,3,3)) as (key,value); -- 多行多列
select posexplode(array(1,2,3)) as (index,value);
select inline(array(struct(1,1),struct(2,2))); 

--lateral view 将炸裂出来的行列， 与源表字段join成一个虚拟表
show create table teacher;
-- teacher.name, teacher.friends, teacher.students, teacher.address, t1.f
select name,friends ,f from teacher lateral view explode(friends) t1 as f;
SELECT * FROM teacher LATERAL VIEW explode(array()) C AS a limit 10;
SELECT * FROM teacher LATERAL VIEW OUTER explode(array()) C AS a limit 10;
set mapreduce.framework.name =local;
--窗口函数
--窗口定义计算范围，函数定义计算逻辑 sum() over() as columnAlias
--窗口范围 基于行 基于值
select id , sum(id) over(order by id rows BETWEEN UNBOUNDED PRECEDING and current row) from student s ;
--基于值 如果起点和终点包含num计算，则排序字段必须是整数
select id , sum(id) over(order by id range BETWEEN 1  PRECEDING and 2 FOLLOWING) from student s ;
select id , sum(id) over(partition by name order by id range BETWEEN 1  PRECEDING and 2 FOLLOWING) from student s ;
--lead 下几行数据 lag获取上几行数据 有默认值  不支持自定义分区 都是基于行的。
select id , lead(id,1,'999') over(order by id) from student s ;
select id , lag(id,1,'999') over(order by id) from student s ;
--获取窗口的第一个和最后一个值，第二个参数是是否跳过null值。
SELECT first_value(id,false) over(order by id) ,last_value(id,false) over(order by id) FROM student s;

insert into table student values(1,'b');
--不支持自定义窗口
select rank() over(order by id),DENSE_RANK () over(order by id),ROW_NUMBER () over(order by id) from student s ;


--创建临时函数
-- add jar /home/bigdata/xx.jar
--create temporary function funcName as "bigdata.hive.udf.LengthDemo";
--创建永久函数
create function lendo2 as "bigdata.hive.udf.LengthDemo" using jar "hdfs://192.168.1.111:8020/udf/hive-1.0-SNAPSHOT.jar";

show functions like "*lendo*";
desc function extended default.lendo2;
select default.lendo2("1324");
