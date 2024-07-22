package cn.hwyee;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext$;
import org.junit.jupiter.api.Test;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName SparkDStreamTest
 * @description 遗留项目，基于rdd，推荐使用structured stream
 * @date 2024/7/8
 * @since JDK 1.8
 */
public class SparkDStreamTest {

    @Test
    void testStart() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("spark").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf,new Duration(1000));

        //对接socket数据源，获取数据，需要启动一个网络服务器。netcat。
        JavaReceiverInputDStream<String> ds = jsc.socketTextStream("localhost", 9999);
        ds.print();


        //启动
        jsc.start();

        ds.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                System.out.println(record);
            });
        });

        //关闭,streaming的数据采集器是长期执行的任务，不能释放
//        jsc.close();

        //等待采集器的关闭
        jsc.awaitTermination();

    }

    @Test
    void testKafka() throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("spark").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf,new Duration(1000));

    }
}
