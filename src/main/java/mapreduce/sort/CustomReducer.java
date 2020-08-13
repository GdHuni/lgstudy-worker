package mapreduce.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
* @功能描述: mr的reduce阶段 入参与map阶段的输出参数一致、输出参数自己定义
* @接口版本: 1.7.4
* @创建作者: <a href="mailto:zhouh@leyoujia.com">周虎</a>
* @创建日期:  2020/8/13 0013 15:30
*
*/
public class CustomReducer extends Reducer<CustomBean, NullWritable,NullWritable, CustomBean> {
    //用来排序的编号
    int id =1;
    @Override
    protected void reduce(CustomBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //遍历相同key的value
        for (NullWritable value : values) {
            //根据自定义排序好的数据来排名
            key.setId(id++);
            context.write(value, key);
        }
    }
}
