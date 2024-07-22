package cn.hwyee.rdd;

import cn.hwyee.util.SparkUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName SparkRDDAction
 * @description 行动算子，相当于stream的终端操作
 * @date 2024/6/22
 * @since JDK 1.8
 */
public class SparkRDDAction {
    private static JavaSparkContext sparkContext;

    @BeforeAll
    public static void init() {
        SparkUtil.conf = new SparkConf()
                .setAppName("My Spark Application")
                .setMaster("local[*]");

        sparkContext = SparkUtil.getSparkContext();
    }

    @AfterAll
    public static void close() {
        SparkUtil.close();
    }

    @Test
    void testCollect() {
        //collect()将RDD转换成数组，将executor结果返回给driver。main方法就是driver端。
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        integerJavaRDD.collect().forEach(System.out::println);
    }

    @Test
    void testCount() {
        //count()计算RDD中元素的个数
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        System.out.println(integerJavaRDD.count());
    }

    @Test
    void testFirst() {
        //first()获取RDD中第一个元素
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        System.out.println(integerJavaRDD.first());
    }

    @Test
    void testTake() {
        //take()获取RDD中前n个元素
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        integerJavaRDD.take(3).forEach(System.out::println);
    }

    @Test
    void testForEach() {
        //foreach()遍历RDD中的每个元素
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);
//        integerJavaRDD.foreach(System.out::println);//org.apache.spark.SparkException: Task not serializable
        //分区内的每个元素都要执行foreach操作，相当于执行n个foreach操作
        integerJavaRDD.foreach(a -> System.out.println(a));
        System.out.println("--------------------------------");
        //占内存 分区内的元素进行一次for循环，执行1个foreachPartition操作
        integerJavaRDD.foreachPartition(iterator -> {
            iterator.forEachRemaining(System.out::println);
        });
    }

    @Test
    void testCache() {
        //cache()将RDD缓存到内存中
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        //rdd重复使用前，缓存起来。
        integerJavaRDD.cache();// = cache() = persist(StorageLevel.MEMORY_ONLY())
        integerJavaRDD.collect().forEach(System.out::println);
    }

    @Test
    void testPersist() {
        //persist()将RDD缓存到文件中,spark自己管理路径
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        integerJavaRDD.persist(StorageLevel.DISK_ONLY());
        integerJavaRDD.collect().forEach(System.out::println);
    }

    /**
     * testCheckpoint:
     * 假设任务在中途失败了，现在我们希望通过 checkpoint 文件来恢复任务。由于 Spark 会自动处理 checkpoint 文件的加载，我们只需确保在重新执行任务时，依然使用同一个 checkpoint 目录。
     *
     * @return void
     * @author hui
     * @version 1.0
     * @date 2024/6/29 22:44
     */
    @Test
    void testCheckpoint() {
        //一般保存在hdfs或者其他分布式文件系统中。
        sparkContext.setCheckpointDir("spark/testcheckpoint");
        //checkpoint()将RDD缓存到文件中
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> map = integerJavaRDD.map(a -> {
            System.out.println("map" + a);
            return a * 2;
        });
        map.cache();//checkpoint为了保证数据的安全性，会重头执行任务，会导致任务执行两遍，所以建议在checkpoint前把任务缓存一下。
        //checkpoint会切断血缘关系。
        map.checkpoint();
        map.collect().forEach(System.out::println);
    }


    @Test
    void testShuffleOperator() {
        //shuffle对性能影响很大，所有shuffle后的rdd自带缓存。
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> map = integerJavaRDD.map(a -> {
            System.out.println("map" + a);
            return a * 2;
        });
        JavaPairRDD<Integer, Integer> JavaPairRDD = map.mapToPair((a) ->
             new Tuple2<Integer, Integer>(a, a*2)
        );
        //这个rdd会被缓存起来，所以这个rdd之前的操作只会执行一遍。 两次连续的相同的shuffle操作，如果分区器也相同（需要实现equals），第二次是不会进行shuffle的。
        JavaPairRDD<Integer, Integer> shuffleCacheRdd = JavaPairRDD.reduceByKey(Integer::sum);
        JavaPairRDD<Integer, Integer> shuffleCacheRdd2 = JavaPairRDD.reduceByKey(Integer::sum);
        shuffleCacheRdd.groupByKey().collect();

        System.out.println("--------------------------------");
        shuffleCacheRdd.sortByKey().collect();
    }

    @Test
    void testAccumulator() {
        LongAccumulator longAccumulator = sparkContext.sc().longAccumulator();
        sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).foreach(a -> {
            longAccumulator.add(a);
        });
        AtomicInteger counter = new AtomicInteger();
        sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).foreach(a -> {
            counter.addAndGet(a);
        });
        System.out.println(counter.get());//0
        System.out.println(longAccumulator.value());//55
    }

    @Test
    void testBroadcast() {
        List<String> list = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
        Broadcast<List<String>> broadcast = sparkContext.broadcast(list);
        sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).foreach(a -> {
            System.out.println(broadcast.value());
        });
    }

}


