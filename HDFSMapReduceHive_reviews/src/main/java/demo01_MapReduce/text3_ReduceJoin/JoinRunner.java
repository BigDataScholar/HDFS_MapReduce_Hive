package demo01_MapReduce.text3_ReduceJoin;

import org.apache.hadoop.conf.Configuration;
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
 * @Date: 2019/11/25 19:19
 * @Description:
 */
public class JoinRunner extends Configured implements Tool {


    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new JoinRunner(), args);
        System.out.println(run);


    }

    @Override
    public int run(String[] strings) throws Exception {

        //1.实例化Configuration对象
        Configuration configuration = new Configuration();
        //初始化job对象
        Job job = new Job(configuration,"ReduceJoin");
        job.setJarByClass(JoinRunner.class);

        //2.设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\2019大数据课程\\DeBug\\测试\\order\\素材\\4\\map端join\\input"));

        //3.设置map
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        //4.设置reduce
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        //5.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\2019大数据课程\\DeBug\\测试结果\\11111111111"));

        boolean b = job.waitForCompletion(true);

        return b?0:1;

    }
}