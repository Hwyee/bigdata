package cn.hwyee.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName WcMapper
 * @description mapred是1.x版本
 * mapreduce是2.x和3.x版本
 * Object,   Text,        Text,   IntWritable
 * 输入key    输入value     输出key   输出value
 * 行首字节偏移
 * @date 2024/5/30
 * @since JDK 1.8
 */
public class WcMapper extends Mapper<Object, Text,Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //context上下文是map与reduce沟通的方式
        String[] words = value.toString().split(" ");
        for (String word : words) {
            context.write(new Text(word),new IntWritable(1));
        }

    }
}
