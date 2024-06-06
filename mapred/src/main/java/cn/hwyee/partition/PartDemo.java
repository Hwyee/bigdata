package cn.hwyee.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName PartDemo
 * @description
 * @date 2024/6/2
 * @since JDK 1.8
 */
public class PartDemo extends Partitioner<LongWritable, LongWritable> {

    @Override
    public int getPartition(LongWritable longWritable, LongWritable longWritable2, int numPartitions) {

        return 0;
    }
}
