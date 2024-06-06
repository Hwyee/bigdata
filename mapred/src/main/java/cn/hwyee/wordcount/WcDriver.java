package cn.hwyee.wordcount;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName WcDriver
 * @description
 * @date 2024/5/30
 * @since JDK 1.8
 */
@Slf4j
public class WcDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取Job
        Configuration configuration = new Configuration();
        Job instance = Job.getInstance(configuration);
        //2.设置jar包路径
//        instance.setJar();
        instance.setJarByClass(WcDriver.class);
        //3.关联map，red
        instance.setMapperClass(WcMapper.class);
        instance.setReducerClass(WcReducer.class);
        //4.设置map输出的key，value
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出的kv
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);

        /*
         * 设置读取文件的Format
         * 如果不设置则默认使用TextInputFormat
         * CombineFileInputFormat可以把小文件从逻辑上划分为一个切片。
         */
//        instance.setInputFormatClass(CombineFileInputFormat.class);
        //虚拟存储文件为10M一个分片 如果>10M < 2*10M 则会均匀的划分为两片 比如剩15M了，则会划分为两片7.5M
//        CombineFileInputFormat.setMaxInputSplitSize(instance,1024*1024*1024);
        //6.设置输入，输出路径
        FileInputFormat.setInputPaths(instance,new Path("./mapred/src/main/resources/wcinput"));
        FileOutputFormat.setOutputPath(instance,new Path("./mapred/build/wcoutput"));
        //7.提交
        boolean b = instance.waitForCompletion(true);
        log.info("程序运行结果:"+(b?"success":"failed"));

    }
}
