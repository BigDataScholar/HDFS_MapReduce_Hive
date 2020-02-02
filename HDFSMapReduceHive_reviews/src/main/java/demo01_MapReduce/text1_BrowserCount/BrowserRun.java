package demo01_MapReduce.text1_BrowserCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/24 18:02
 * @Description:
 */
public class BrowserRun {

    public static void main(String[] args) throws Exception {


        //1.初始化job对象
        Job job = new Job();

        //2.设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\2019大数据课程\\day\\大数据\\作业\\练习题\\access.log"));

        //3.设置Map
        job.setMapperClass(BrowserMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Browser.class);

        //4.设置Reduce
        job.setReducerClass(BrowserReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\2019大数据课程\\DeBug\\测试结果\\browser_count"));

        boolean b = job.waitForCompletion(true);

        System.out.println(b?0:1);

    }
}