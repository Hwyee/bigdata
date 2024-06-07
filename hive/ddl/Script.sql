show databases;

/**
 CREATE [REMOTE] (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
  [COMMENT database_comment]
  [LOCATION hdfs_path]
  [MANAGEDLOCATION hdfs_path]
  [WITH DBPROPERTIES (property_name=property_value, ...)];
 */
CREATE DATABASE IF NOT EXISTS hive_db1 
COMMENT "this is a hive db. one" 
LOCATION "/ hive / hive1" 
WITH DBPROPERTIES ("create_user" = "hwyee",
"create_time" = "2024-06-07");

select
	CURRENT_DATABASE();

use default;
--显示数据库创建信息，包括dbproperties
desc database extended hive_db1;
--数据类型转换
SELECT
	'1' + 2 L;
--double类型
SELECT
	CAST('1' as int);
--建表
create table if not exists default.student(
	id int comment 'id',
	name string comment 'name'
)
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/student';

select
	*
from
	default.student;
--serde test
create table if not exists default.teacher(
	name string,
	friends array<string>,
	students map<string,
int>,
	address struct<street:string,
city:string,
postal_code:int>
)
row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe'
location '/user/hive/warehouse/teacher';

select
	*
from
	default.teacher;
-- array取值、map取值、struct取值
select
	friends[0],
	students['xiaohaihai'],
	address.street
from
	default.teacher;
--create table use as select ...  需要关闭虚拟内存检查 否则可能内存不够
create table default.teacher1 as
select
	*
from
	default.teacher;

select
	*
from
	default.teacher1;
--有数据
--create table like othertable
create table default.teacher2 like default.teacher;

select
	*
from
	default.teacher2;
--空数据
-- 没有like也可以
show tables in default like "stu *";

describe extended default.teacher;
-- 用formatted 
describe formatted teacher;
--修改表
alter table default.teacher1 rename to t1;

select
	*
from
	t1;
--修改列
--新增
alter table default.t1 add columns (course string comment "course");

select
	*
from
	t1;
--修改 字段类型得兼容 可以设置参数不做校验,但是如果数据强转不过来，也会报错
--set hive.metastore.disallow.incompatible.col.type.changes=false;
alter table t1 change column address addr struct<street:string,
city:string,
postal_code:int> ;
--first | after column;
select
	*
from
	t1;
--替换 替换所有列
alter table t1 replace columns (id string,
name string);

select
	*
from
	t1;
	
--删除表
drop table if exists t1;
-- select  * from t1;
truncate table teacher2;
