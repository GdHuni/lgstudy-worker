package mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
* @功能描述: mr mapper类 定义从文件输入的入参 以及输出到reduce阶段的输出参数
* @接口版本: 1.0.0
* @创建作者: 周虎
* @创建日期:  2020/8/13 0013 15:28
*
*/
public class CustomMapper extends Mapper<LongWritable, Text, CustomBean, NullWritable> {
    CustomBean customBean = new CustomBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String pNum  = value.toString();
        customBean.setNum(Integer.parseInt(pNum));
        context.write(customBean,NullWritable.get());
    }
}
