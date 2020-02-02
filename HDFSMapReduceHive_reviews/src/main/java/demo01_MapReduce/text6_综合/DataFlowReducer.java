package demo01_MapReduce.text6_综合;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 10:19
 * @Description:
 */
public class DataFlowReducer extends Reducer<Text,DataFlow,Text,DataFlow> {

    //实例化一个对象,用来保存最终的数据
    DataFlow dataFlow = new DataFlow();

    @Override
    protected void reduce(Text key, Iterable<DataFlow> values, Context context) throws IOException, InterruptedException {


        //初始化几个属性
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;


        //遍历相同手机号的list集合
        for (DataFlow value : values) {

            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();

        }

        //赋值
        dataFlow.setUpFlow(upFlow);
        dataFlow.setDownFlow(downFlow);
        dataFlow.setUpCountFlow(upCountFlow);
        dataFlow.setDownCountFlow(downCountFlow);

        //将结果输出
        context.write(key,dataFlow);


    }
}