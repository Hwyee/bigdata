package cn.hwyee.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName WcReducer
 * @description
 * @date 2024/5/30
 * @since JDK 1.8
 */
public class WcReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
