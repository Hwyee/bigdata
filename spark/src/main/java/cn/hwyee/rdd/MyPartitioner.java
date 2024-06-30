package cn.hwyee.rdd;

import org.apache.spark.Partitioner;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName MyPartitioner
 * @description
 * @date 2024/6/30
 * @since JDK 1.8
 */
public class MyPartitioner extends Partitioner {
    private static  int a = 1;
    /**
     * numPartitions:
     * 指定分区数量
     * @author hui
     * @version 1.0
     * @return int
     * @date 2024/6/30 1:05
     */
    @Override
    public int numPartitions() {
        return 10;
    }

    @Override
    public int getPartition(Object key) {
        return a++%10;
    }
}
