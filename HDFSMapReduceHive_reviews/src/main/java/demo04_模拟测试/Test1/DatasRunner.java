package demo04_模拟测试.Test1;

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
 * @Date: 2019/11/26 19:42
 * @Description:
 */
public class DatasRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new DatasRunner(), args);
        System.out.println("status:"+run);

    }


    @Override
    public int run(String[] strings) throws Exception {

        //1.实例化job对象

        Job job = new Job();
        job.setJarByClass(DatasRunner.class);


        //2.设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://192.168.100.100:8020/bb/datas1.txt"));

        //3.设置map
        job.setMapperClass(DatasMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //4.设置reduce
        job.setReducerClass(DatasReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //5.设置输出
        job.setOutputValueClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.100.100:8020/cc/datas/"));



        //设置reduceTask数量
        job.setNumReduceTasks(2);
        //设置分区自定义类
        job.setPartitionerClass(MyPartition.class);



        //等待执行
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }
}