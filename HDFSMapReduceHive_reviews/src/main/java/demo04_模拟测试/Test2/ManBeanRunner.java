package demo04_模拟测试.Test2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 20:24
 * @Description:
 */
public class ManBeanRunner extends Configured implements Tool {


    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new ManBeanRunner(), args);

        System.out.println("status:"+run);

    }
    @Override
    public int run(String[] strings) throws Exception {


        //1.实例化一个job
        Job job = new Job();

        //2.设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\2019大数据课程\\day\\大数据\\4Hive\\模拟考试\\2"));

        //3.设置map
        job.setMapperClass(MeanBeanMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ManBean.class);

        //4.设置reduce
        job.setReducerClass(MeanBeanReducer.class);
        job.setOutputKeyClass(ManBean.class);
        job.setOutputValueClass(NullWritable.class);

        //5.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\2019大数据课程\\day\\大数据\\4Hive\\模拟考试\\2_answer"));

        //6.等待执行

        return job.waitForCompletion(true)?0:1;

    }
}