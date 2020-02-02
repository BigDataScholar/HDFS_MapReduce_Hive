package demo01_MapReduce.text4_MapJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 20:12
 * @Description:
 */
public class JoinMapper extends Mapper<LongWritable, Text,Text,Text> {

    //定义两个全局变量
    HashMap<String,String> hashMap = new HashMap<>();

    String line = null;



    /*
    map端的初始化方法，获取缓存文件,一次性加载到map当中来
     */

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {


        //获取所有的缓存文件
        URI[] cacheFiles = DistributedCache.getCacheFiles(context.getConfiguration());

        //获取map的缓存文件
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());

        //打开缓存文件
        FSDataInputStream open = fileSystem.open(new Path(cacheFiles[0]));

        //创建缓冲流对象进行读取
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open));

        while ((line = bufferedReader.readLine())!=null){

            String[] split = line.split(",");
            //把数据存放在map集合中
            hashMap.put(split[0],split[1]+"\t"+split[2]+"\t"+split[3]);

        }

        fileSystem.close();
        IOUtils.closeStream(bufferedReader);


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //这里读的这个map task 所负责的那一个切片数据(在hdfs)
        String string = value.toString();
        String[] field = string.split(",");

        String orderId = field[0];
        String date =  field[1];
        String pdId = field[2];
        String amount = field[3];

        String info = hashMap.get(pdId);

        context.write(new Text(pdId),new Text(orderId+"-"+date+"-"+info));


    }
}