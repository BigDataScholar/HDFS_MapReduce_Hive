package demo04_模拟测试.Test2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 20:12
 * @Description:
 */
public class MeanBeanReducer extends Reducer<Text,ManBean,ManBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<ManBean> values, Context context) throws IOException, InterruptedException {

        //初始化一个对象用来保存最后的结果
        ManBean manBean = new ManBean();

        for (ManBean value : values) {

            if (!value.getName().equals("null")&&value.getName()!=null){
                manBean.setId(value.getId());
                manBean.setName(value.getName());


            }else  if (!value.getSex().equals("null")&&value.getSex()!=null){
                manBean.setSex(value.getSex());


            }else if (!value.getAge().equals("null")&&value.getAge()!=null){
                manBean.setAge(value.getAge());


            }
        }

        //将最后的结果输出
        context.write(manBean,NullWritable.get());


    }
}