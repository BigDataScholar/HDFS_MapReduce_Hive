package demo01_MapReduce.text6_综合;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 09:46
 * @Description:
 */
public class MyPartition extends Partitioner<Text,DataFlow> {


    //分区逻辑代码
    @Override
    public int getPartition(Text text, DataFlow dataFlow, int i) {

        //这里对key手机号做一个判断
        //以手机号开头为“135”、“136”、“137”、“138”及其他作为分区条件，将结果写入5个不同的文件


        //将text类型的数据转换成string类型的
        String substring = text.toString();


        if (substring.startsWith("135")){

            return 0;

        }else if (substring.startsWith("136")){
            return 1;

        }else if (substring.startsWith("137")){
            return 2;

        }else if (substring.startsWith("138")){
            return 3;

        }else {
            return 4;

        }

    }
}