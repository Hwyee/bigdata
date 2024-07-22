package cn.hwyee.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName SparkUtil
 * @description
 * @date 2024/6/17
 * @since JDK 17
 */
@Slf4j
public class SparkUtil {
    private static  JavaSparkContext javaSparkContext;

    private static SparkSession sparkSession;
    public static  SparkConf conf = new SparkConf();
    public static void main(String[] args) {

    }

    public static SparkSession getDefaultSparkSession() {
        sparkSession = SparkSession.builder().master("local").appName("SparkUtil").getOrCreate();
        return sparkSession;
    }

    public static SparkSession getSparkSession() {
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
        return sparkSession;
    }

    public static SparkSession getSparkSession(SparkConf conf) {
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
        return sparkSession;
    }


    public static JavaSparkContext getDefaultSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SparkUtil");
        javaSparkContext = new JavaSparkContext(conf);
        return javaSparkContext;
    }

    public static JavaSparkContext getSparkContext() {
        javaSparkContext = new JavaSparkContext(conf);
        return javaSparkContext;
    }

    public static JavaSparkContext getSparkContext(SparkConf conf) {
        javaSparkContext = new JavaSparkContext(conf);
        return javaSparkContext;
    }

    public static void saveAsFile(JavaRDD javaRDD){
        javaRDD.saveAsTextFile("spark/testrddout/" + System.currentTimeMillis());
    }

    public static void saveAsFile(JavaRDD javaRDD,String path){
        javaRDD.saveAsTextFile("spark/testrddout/" +path+ System.currentTimeMillis());
    }



    public static void close(){
        javaSparkContext.close();
    }
    public static void closeSql(){
        sparkSession.close();
    }

    public static void saveAsFile(JavaPairRDD<String, Integer> javaPairRDD, String path) {
        javaPairRDD.saveAsTextFile("spark/testrddout/" +path+ System.currentTimeMillis());
    }
}
