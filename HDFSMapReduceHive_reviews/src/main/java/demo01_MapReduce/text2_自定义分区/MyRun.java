package demo01_MapReduce.text2_自定义分区;

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
 * @Date: 2019/11/25 09:05
 * @Description:
 *
 */
public class MyRun extends Configured implements Tool {


    public static void main(String[] args) throws Exception {


        int run = ToolRunner.run(new Configuration(), new MyRun(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {

        //1.初始化job对象
        Job job = Job.getInstance(super.getConf(),MyRun.class.getSimpleName());

        //2.设置输入
        job.setJarByClass(MyRun.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://192.168.100.100:8020/tmp/partition.txt"));

        //3.设置map
        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //4.设置reduce
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //5.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.100.100:8020/aa"));

        //设置分区数量以及map reduce 数量
        job.setPartitionerClass(MyPartition.class);
        job.setNumReduceTasks(2);

        //6.等待结果
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }
}