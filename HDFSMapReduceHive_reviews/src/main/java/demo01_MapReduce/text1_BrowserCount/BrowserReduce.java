package demo01_MapReduce.text1_BrowserCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/24 18:01
 * @Description:
 */
public class BrowserReduce extends Reducer<Text,Browser,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Browser> values, Context context) throws IOException, InterruptedException {

        //初始化一个变量用来保存浏览器的使用次数
        int num = 0;

        //遍历统计个数
        for (Browser value : values) {
            num ++;
        }

        context.write(key,new IntWritable(num));

    }
}