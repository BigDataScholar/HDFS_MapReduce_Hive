package demo04_模拟测试.Test1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 19:42
 * @Description:
 */
public class DatasMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String string = value.toString();

        String[] split = string.split("\t");

        String name = split[2];

        if (name.length()>5 ){
            //如果名字长度大于5就输出数据

            context.write(value,NullWritable.get());
        }

    }
}