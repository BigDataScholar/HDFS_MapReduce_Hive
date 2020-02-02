package demo01_MapReduce.text3_ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 18:03
 * @Description:
 */
public class JoinMapper extends Mapper<LongWritable, Text,Text,JoinBean> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //实例化一个JoinBean 对象,用来封装每行读取的数据
        JoinBean joinBean = new JoinBean();

        //通过context可以获取到这行文本所属的文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        //获取文件的名字
        String filename = inputSplit.getPath().getName();

        //将map输入的text类型的数据准换成string类型的
        String str = value.toString();

        String[] split = str.split(",");


        if (filename.contains("order")){
           //读取的是订单.txt文件
            joinBean.setId(split[0]);
            joinBean.setDate(split[1]);
            joinBean.setPid(split[2]);
            joinBean.setAmount(split[3]);

            //将多个文件中都存在的属性作为key 输出
            context.write(new Text(split[2]),joinBean);

        }else {
            //读取的是商品product.txt
            joinBean.setPname(split[1]);
            joinBean.setCategory_id(split[2]);
            joinBean.setPrice(split[3]);

            context.write(new Text(split[0]),joinBean);

        }




    }
}