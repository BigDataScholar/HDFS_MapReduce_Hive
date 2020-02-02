package demo04_模拟测试.Test2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 20:12
 * @Description:
 */
// 如果在 Mapper 输出阶段将 bean 作为 key,则 对应的 bean 类必须要实现WritableComparble 接口
public class MeanBeanMapper extends Mapper<LongWritable, Text,Text,ManBean> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //初始化一个ManBean对象
        ManBean manBean = new ManBean();

        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        //获取当前行所在的文件名
        String name = fileSplit.getPath().getName();

        //把Text类型的数据转换成String类型的
        String string = value.toString();

        String[] split = string.split("\t");

        if (name.contains("1")){

            //读取一号文件   ---》姓名	编号
            manBean.setName(split[0]);
            manBean.setId(split[1]);

            //输出
            context.write(new Text(split[1]),manBean);

        }else if (name.contains("2")){
            //读取二号文件  ---》 编号  性别
            String[] split1 = string.split(",");
            manBean.setSex(split1[1]);
            context.write(new Text(split1[0]),manBean);


        }else if (name.contains("3")){
            //读取三号文件  ---》 编号  年龄
            String[] split1 = string.split("\\|");
            manBean.setAge(split1[1]);
            context.write(new Text(split1[0]),manBean);

        }


    }
}