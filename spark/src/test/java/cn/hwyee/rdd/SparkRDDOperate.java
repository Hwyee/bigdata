package cn.hwyee.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import scala.Tuple22;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName SparkRDDOperate
 * @description 转换算子，相当于stream的中间操作
 * @date 2024/6/17
 * @since JDK 17
 */

public class SparkRDDOperate {
    private static JavaSparkContext sparkContext;

    @BeforeAll
    public static void init() {
        SparkUtil.conf = new SparkConf()
                .setAppName("My Spark Application")
                .setMaster("local[*]");
//                .set("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
//                .set("spark.executor.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")

        sparkContext = SparkUtil.getSparkContext();
    }

    @AfterAll
    public static void close() {
        SparkUtil.close();
    }

    @Test
    void testList() {
        List<Integer> list = Arrays.asList(1, 2, 3);
        //第二个参数是切片数量 默认值：spark.default.parallelism,这个值如果没有就是cpu核数
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(list, 2);
        integerJavaRDD.collect().forEach(System.out::println);
        integerJavaRDD.saveAsTextFile("testrddout/" + System.currentTimeMillis());
    }

    @Test
    void testFile() {
        //文件数据源分区 第二个参数是最小分区数，默认值spark.default.parallelism
        //文件是按行读数据的，按字节数进行分区
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("testrdd.txt", 3);
        stringJavaRDD.collect().forEach(System.out::println);
        stringJavaRDD.saveAsTextFile("testrddout/" + System.currentTimeMillis());
    }

    @Test
    void testMap() {
        new Tuple2<String, String>("key", "value");
        List<String> list = Arrays.asList("1", "abc", "hello");
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(list, 2);
        //默认新创建的rdd分区数和旧的一样
        JavaRDD<Integer> integerJavaRDD = stringJavaRDD.map(String::length);
        integerJavaRDD.collect().forEach(System.out::println);
    }

    @Test
    void testFilter() {
        List<String> list = Arrays.asList("1", "abc", "hello");
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(list, 2);
        JavaRDD<String> filterJavaRDD = stringJavaRDD.filter(s -> s.length() > 1);
        filterJavaRDD.collect().forEach(System.out::println);
        System.out.println();
        //过滤后可能会导致每个分区的数据发生倾斜，慎用。
        stringJavaRDD.filter(
                s -> {
                    System.out.println("fileter.." + s);
                    return s.length() > 1;
                }).map(
                s -> {
                    System.out.println("map.." + s);
                    return s.toUpperCase();
                }).collect().forEach(System.out::println);

    }

    /**
     * testFlatMap:
     * 扁平化映射
     *
     * @return void
     * @author hui
     * @version 1.0
     * @date 2024/6/18 0:03
     */
    @Test
    void testFlatMap() {
        List<String> list = Arrays.asList("1", "abc", "hello");
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(list, 2);
        JavaRDD<String> flatMapJavaRDD = stringJavaRDD.flatMap(s -> Arrays.asList(s.split("")).iterator());
        flatMapJavaRDD.collect().forEach(System.out::println);
    }

    @Test
    void testGroupBy() {
        List<String> list = Arrays.asList("1", "abc", "hello", "world");
        //group by会改变分区，同一个组的数据必须在同一个分区，但是一个分区可以有多个组。
        //分组操作会将数据分区打乱重新组合，这个操作是spark的shuffle操作.shuffle会落盘
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(list, 2);
        JavaPairRDD<Object, Iterable<String>> groupByJavaRDD = stringJavaRDD.groupBy(s -> s.length());
        groupByJavaRDD.collect().forEach(System.out::println);

    }

    @Test
    void testStream() {
        System.out.println(System.currentTimeMillis());
        Stream<String> stringStream = Stream.of("1", "abc", "hello", "world").filter((a) -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return true;
        });
        Stream<String> stringStream1 = stringStream.flatMap(a -> Arrays.stream(a.split("")));
//        stringStream.forEach(System.out::println);
        System.out.println(System.currentTimeMillis());

        Stream.of("1", "abc", "hello", "world").filter((a) -> {
            System.out.println("filter " + a);
            return true;
        }).map((a) -> {
            System.out.println("map " + a);
            return a.length();
        }).forEach(System.out::println);
        System.out.println(System.currentTimeMillis());
    }

    @Test
    void testDistinct() {
        List<String> list = Arrays.asList("1", "abc", "hello", "world", "1", "abc", "hello", "world");
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(list, 2);
        JavaRDD<String> distinctJavaRDD = stringJavaRDD.distinct();
        distinctJavaRDD.collect().forEach(System.out::println);
    }

    @Test
    void testSort() {
        List<String> list = Arrays.asList("1", "abc", "hello", "world", "1", "abc", "hello", "world");
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(list, 2);
        JavaRDD<String> sortJavaRDD = stringJavaRDD.sortBy(String::length, true, 2);
        sortJavaRDD.collect().forEach(System.out::println);
    }

    /**
     * testKeyValue:
     * key value 数据处理，只对key或者value进行处理，而不是对整体进行处理。
     *
     * @return void
     * @author hui
     * @version 1.0
     * @date 2024/6/18 18:05
     */
    @Test
    void testKeyValue() {
        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2));
        JavaPairRDD<String, Integer> stringJavaRDD = sparkContext.parallelizePairs(list, 2);
        //key 不变把value转换成99999
        JavaPairRDD<String, Integer> pairRDD = stringJavaRDD.mapValues(i -> 99999);
        pairRDD.collect().forEach(System.out::println);
    }

    @Test
    void testGroupByKey() {
        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2),
                new Tuple2<>("a", 2), new Tuple2<>("b", 5));
        JavaPairRDD<String, Integer> stringJavaRDD = sparkContext.parallelizePairs(list, 2);
        //根据key对value进行聚合
        // (b,[2, 5])
        //(a,[1, 2])
        JavaPairRDD<String, Iterable<Integer>> groupByKeyJavaRDD = stringJavaRDD.groupByKey();
        groupByKeyJavaRDD.collect().forEach(System.out::println);
    }

    @Test
    void testReduceByKey() {
        System.out.println(System.currentTimeMillis());
        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2),
                new Tuple2<>("a", 2), new Tuple2<>("b", 5), new Tuple2<>("a", 10));
        JavaPairRDD<String, Integer> stringJavaRDD = sparkContext.parallelizePairs(list, 2);
        //根据key对value进行聚合,在shuffle之前会进行分区内的聚合，提升性能
        //(b,7)
        //(a,13)
        JavaPairRDD<String, Integer> reduceByKeyJavaRDD = stringJavaRDD.reduceByKey(Integer::sum);
        System.out.println(System.currentTimeMillis());
        reduceByKeyJavaRDD.collect().forEach(System.out::println);
    }

    @Test
    void testSortByKey() {
        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("a", 10), new Tuple2<>("b", 2),
                new Tuple2<>("a", 2), new Tuple2<>("b", 5));
        JavaPairRDD<String, Integer> stringJavaRDD = sparkContext.parallelizePairs(list, 2);
        //根据key进行排序，a在前b在后，value不排序。
        JavaPairRDD<String, Integer> sortByKeyJavaRDD = stringJavaRDD.sortByKey(true, 2);
        sortByKeyJavaRDD.collect().forEach(System.out::println);
    }

    @Test
    void testCoalesce() {
        List<String> list = Arrays.asList("1", "abc", "hello", "world", "1", "abc", "hello", "world");
        JavaRDD<String> stringJavaRDD = sparkContext.parallelize(list, 2);
        //缩减分区，如果要扩大分区，需要进行shuffle，第二个参数就是是否进行shuffle。
        JavaRDD<String> coalesceJavaRDD = stringJavaRDD.coalesce(4);
        coalesceJavaRDD.collect().forEach(System.out::println);
        //扩大分区如果未执行shuffle，则不会进行分区。
        SparkUtil.saveAsFile(coalesceJavaRDD, "testCoalesce1/" );
        //重分区，底层也是调用coalesce，第二个参数为true
        JavaRDD<String> repartitionJavaRDD = stringJavaRDD.repartition(3);
        //执行shuffle会进行shuffle，这里分区从2变成了3.
        SparkUtil.saveAsFile(repartitionJavaRDD, "testCoalesce2/" );
    }

}
