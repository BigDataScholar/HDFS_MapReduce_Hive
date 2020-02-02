package demo04_模拟测试.Test1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 19:42
 * @Description:
 */
public class DatasReducer extends Reducer<Text, NullWritable,Text,NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //输出数据
        context.write(key,NullWritable.get());

    }
}