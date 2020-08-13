package mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
* @功能描述: mr任务启动类
* @接口版本: 1.0.0
* @创建作者: 周虎
* @创建日期:  2020/8/13 0013 15:28
*
*/
public class Customjob {
    public static void main(String[] args) throws Exception {
        //1.创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.获取jar运行路径
        job.setJarByClass(Customjob.class);
        //3.获取mapper类
        job.setMapperClass(CustomMapper.class);
        //4.获取reduce类
        job.setReducerClass(CustomReducer.class);
        //5.获取mapper输出类型
        job.setMapOutputKeyClass(CustomBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //6.获取最终输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(CustomBean.class);
        //7.获取输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path("f://input"));
        FileOutputFormat.setOutputPath(job, new Path("f://out111"));
        //8.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
