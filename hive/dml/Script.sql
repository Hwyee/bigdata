-- 192.168.1.111 中的新脚本。
-- 
-- 日期：2024年6月7日
-- 
-- 时间：下午10:56:05


--load语句 local是本地路径（hive服务器本地），否则是hdfs,如果是hdfs则导入完后源文件消失
load data local inpath '/home/bigdata/hive/teacher.txt' into table teacher;
select * from teacher;

--insert into\overwrite
insert into table teacher select * from teacher; 
select * from teacher;--数据翻倍
--插入values Hive 不支持复杂类型（array、map、struct、union）的字面意义，因此无法在 INSERT INTO...VALUES 子句中使用它们。这意味着用户无法使用 INSERT INTO...VALUES 子句向复杂数据类型列插入数据。
--insert into table teacher values  ("aa",null,null,null);
insert into table student values (123,"abc");
select * from student; 

--插入到文件中 最好写不存在的目录，否则会覆盖目录下的所有文件 指定的是目录不是文件！
insert overwrite local directory "/home/bigdata/hive/t1" select * from teacher;

--导入导出
--导出
export table teacher to '/user/hive/warehouse/exporttea';
--导入
import table tea2 from '/user/hive/warehouse/exporttea';
select * from tea2;

