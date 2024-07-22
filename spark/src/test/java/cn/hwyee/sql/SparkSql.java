package cn.hwyee.sql;

import cn.hwyee.bean.User;
import cn.hwyee.util.SparkUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.AtomicType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StringType$;
import org.glassfish.hk2.api.Self;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName SparkSql
 * @description
 * @date 2024/7/1
 * @since JDK 1.8
 */

public class SparkSql {
    private static SparkSession sparkSession = null;

    @BeforeAll
    public static void init() {
        SparkConf conf = new SparkConf().setAppName("SparkSql").setMaster("local[*]");
        sparkSession = SparkUtil.getSparkSession(conf);
    }

    @AfterAll
    public static void close() {
        SparkUtil.closeSql();
    }

    @Test
    void testReadJson() {
        Dataset<Row> json = sparkSession.read().json("sql/test.json");
        JavaRDD<Row> rowJavaRDD = json.javaRDD();
        json.show();
    }

    @Test
    void testSql() {
        Dataset<Row> ds = sparkSession.read().json("sql/test.json");
        //将数据模型转换成表(视图)
        ds.createOrReplaceTempView("json");
        sparkSession.sql("select * from json").show();
        sparkSession.sql("select * from json where age > 20").show();
        //转化成Dataset<User>
        Dataset<User> userDataset = ds.as(Encoders.bean(User.class));
        userDataset.show();
    }

    @Test
    void testDSl() {
        Dataset<Row> ds = sparkSession.read().json("sql/test.json");
        ds.show();
        ds.printSchema();
        ds.select("name").show();

    }

    @Test
    void testUDF() {

        sparkSession.udf().register("myUdf", (UDF1<String, String>) s -> "myUdf:" + s, DataTypes.StringType);//StringType$.MODULE$
        Dataset<Row> ds = sparkSession.read().json("sql/test.json");
        ds.createOrReplaceTempView("test");
        sparkSession.sql("select myUdf(name) from test").show();
//        ds.select("myUdf(name)").show();org.apache.spark.sql.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `myUdf(name)` cannot be resolved.

    }

    @Test
    void testUDAF() {
        //udaf 自定义聚合函数 两个参数：udaf对象和输入类型
        sparkSession.udf().register("myUdaf", functions.udaf(new MyUdaf(), Encoders.LONG()));
        Dataset<Row> ds = sparkSession.read().json("sql/test.json");
        ds.createOrReplaceTempView("test");
        sparkSession.sql("select myUdaf(age) from test").show();
    }

    @Test
    void testCsv() {
        Dataset<Row> ds = sparkSession
                .read()
                .option("header", "true")//第一行是表头
                .option("sep", ",")//指定分隔符，默认就是逗号,
                .csv("sql/test.csv");
        long count = ds.count();
        System.out.println(count);
        ds.show();
        //如果输入文件夹已经存在，则spark默认会报错，可以修改配置
        ds.write()
                .option("header", "true")
                .option("sep", "\t")
                .mode(SaveMode.Append)//文件夹存在也可以保存，追加文件。还有append，ignore,error策略。默认值就是error
                .csv("output/csv");

        ds.write()
                .option("header", "true")
                .mode(SaveMode.Append)
                .parquet("output/parquet");
    }


    @Test
    void testHive(){
        sparkSession.sql("show databases").show();//default
    }


}
