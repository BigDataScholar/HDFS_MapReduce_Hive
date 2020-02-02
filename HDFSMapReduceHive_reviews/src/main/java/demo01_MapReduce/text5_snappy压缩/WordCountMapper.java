package demo01_MapReduce.text5_snappy压缩;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 08:25
 * @Description:
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //将Text数据转换成String类型的数据
        String string = value.toString();

        //将数据进行切割
        String[] s = string.split(" ");

        //遍历将每个字母输出
        for (String s1 : s) {

            //输出结果
            context.write(new Text(s1),new LongWritable(1));

        }

    }
}