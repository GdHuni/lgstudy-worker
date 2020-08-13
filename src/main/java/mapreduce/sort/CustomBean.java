package mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
* @功能描述: 自定义实体类
* @接口版本: 1.0.0
* @创建作者: 周虎
* @创建日期:  2020/8/13 0013 15:27
*
*/
public class CustomBean implements WritableComparable<CustomBean> {
    private int id;
    private int num;

    // 反序列化时，需要反射调用空参构造函数，所以必须有
    public CustomBean() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    //作业需要用\t来分割
    @Override
    public String toString() {
        return
                id + "\t"+ num;
    }

    /**
     *  序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(num);
    }

    /**
     *  反序列化方法，接收参数的顺序要与序列化的顺序一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        num=in.readInt();
    }

    //重写比较方法,1为正序,-1为倒序
    @Override
    public int compareTo(CustomBean o) {
        return this.num>o.getNum() ? 1:-1;
    }
}
