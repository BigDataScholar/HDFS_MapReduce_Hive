package demo01_MapReduce.text4_MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 20:12
 * @Description:
 */
public class JoinRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new Configuration(), new JoinRunner(), args);
        System.out.println("status:"+run);

    }
    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = new Configuration();

        //1.设置缓存文件【必须放在集群上,一般是小文件】
        DistributedCache.addCacheFile(new URI("hdfs://192.168.100.100/tmp/pdts.txt"),conf);

        //2.实例化job对象
        Job job = Job.getInstance(conf, "MapReduce");

        //3.设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\2019大数据课程\\DeBug\\测试\\order\\素材\\4\\map端join\\map_join_iput\\orders.txt"));

        //4.设置map
       job.setMapperClass(JoinMapper.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);

       //5.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\2019大数据课程\\DeBug\\测试结果\\mapjoin"));

        //Map Join 因为没有用到reduce,因此可以不用设置reduce

        return job.waitForCompletion(true)?0:1;
    }
}