package demo01_MapReduce.text6_综合;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 09:46
 * @Description:
 */
public class DataFlowMapper extends Mapper<LongWritable, Text,Text,DataFlow> {


    //初始化一个DataFlow对象
    DataFlow dataFlow = new DataFlow();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        //将Text类型的行数据转换成String
        String string = value.toString();

        String[] split = string.split("\t");

        //手机号
        String phone = split[0];

        //上传流量
        String upFlow = split[6];

        //下载流量
        String downFlow = split[7];

        //上传总流量
        String upCountFlow = split[8];

        //下载总流量
        String downCountFlow = split[9];

        //为对象dataFlow赋值
        dataFlow.setUpFlow(Integer.parseInt(upFlow));
        dataFlow.setDownFlow(Integer.parseInt(downFlow));
        dataFlow.setUpCountFlow(Integer.parseInt(upCountFlow));
        dataFlow.setDownCountFlow(Integer.parseInt(downCountFlow));


        System.out.println(dataFlow);

        //将手机号作为key,dataFlow作为value输出数据
        context.write(new Text(phone),dataFlow);


    }
}