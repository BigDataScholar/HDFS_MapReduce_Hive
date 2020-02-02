package demo04_模拟测试.Test1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 19:47
 * @Description:
 */
public class MyPartition extends Partitioner<Text, NullWritable> {



    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {


        //将Test类型的数据转换成String类型的数据
        String string = text.toString();

        //分割字符串
        String[] split = string.split("\t");

        //获取进度
        String speed = split[0];

        if (Integer.parseInt(speed) < 50){

         return  0;


        }else{

            return 1;


        }

    }
}