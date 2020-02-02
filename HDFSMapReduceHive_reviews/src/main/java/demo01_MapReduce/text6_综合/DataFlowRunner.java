package demo01_MapReduce.text6_综合;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 10:32
 * @Description:
 */
public class DataFlowRunner extends Configured implements Tool {


    public static void main(String[] args) throws Exception {


        int run = ToolRunner.run(new DataFlowRunner(),args);
        System.out.println("status:"+run);

    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration configuration = new Configuration();

//        configuration.set("mapreduce.map.output.compress","true");
//        configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
//        //设置reduce阶段的压缩
//        configuration.set("mapreduce.output.fileoutputformat.compress","true");
//        configuration.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
//        configuration.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");


        //1.实例化job对象
        Job job = Job.getInstance(configuration, "DataFlow");
        job.setJarByClass(DataFlowRunner.class);

        //2.设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/bb/data_flow3.1.dat"));

        //3.设置map
        job.setMapperClass(DataFlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataFlow.class);

        //4.设置reduce
        job.setReducerClass(DataFlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataFlow.class);

        //5.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/cc/DataFlow/"));

        //设置分区
        job.setPartitionerClass(MyPartition.class);

        //设置reduceTask个数
        job.setNumReduceTasks(5);

        //等待执行
        boolean b = job.waitForCompletion(true);


        return b?0:1;
    }
}