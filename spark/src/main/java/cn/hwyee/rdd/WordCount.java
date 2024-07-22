package cn.hwyee.rdd;

import cn.hwyee.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName WordCount
 * @description
 * @date 2024/6/18
 * @since JDK 17
 */
@Slf4j
public class WordCount {

    public static void main(String[] args) {
        JavaSparkContext defaultSparkContext = SparkUtil.getDefaultSparkContext();
        JavaRDD<String> stringJavaRDD = defaultSparkContext.textFile("spark/wordcount.txt", 3);
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringJavaRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .groupBy(s -> s)
                .mapValues(iterable -> {
                            int sum = 0;
                            for (String ignored : iterable) {
                                sum++;
                            }
                            return sum;
                        }
                );

        stringIntegerJavaPairRDD.collect().forEach(System.out::println);
        SparkUtil.saveAsFile(stringIntegerJavaPairRDD,"wordcount/");
    }
}
