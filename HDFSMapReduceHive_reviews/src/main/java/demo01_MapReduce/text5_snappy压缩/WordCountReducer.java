package demo01_MapReduce.text5_snappy压缩;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 08:25
 * @Description:
 */
public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {


        //初始化一个变量用来保存每个字母出现的次数
        int num = 0 ;

        for (LongWritable value : values) {

            //.get()方法可以将longwrite转换成int类型
            num+=value.get();

        }

        //输出结果
        context.write(key,new LongWritable(num));

    }
}