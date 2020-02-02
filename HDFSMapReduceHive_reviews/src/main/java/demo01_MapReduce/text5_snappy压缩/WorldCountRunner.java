package demo01_MapReduce.text5_snappy压缩;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 08:25
 * @Description:
 *               压缩案例只能放在【集群】上面运行
 */
public class WorldCountRunner extends Configured implements Tool {


    public static void main(String[] args) throws Exception {



        int run = ToolRunner.run(new WorldCountRunner(), args);

        System.out.println("status:"+run);

    }


    @Override
    public int run(String[] strings) throws Exception {

        Configuration configuration = new Configuration();

        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
        //设置reduce阶段的压缩
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        //1.实例化job对象
        //Job job = new Job();
        Job job = Job.getInstance(configuration, "SNAPPY");

        job.setJarByClass(WorldCountRunner.class);

        //2.设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/bb/wordcount.txt"));

        //3.设置map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        //4.设置reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //5.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/cc/wordcountresult/"));

        //等待执行

        return job.waitForCompletion(true)?0:1;


    }
}