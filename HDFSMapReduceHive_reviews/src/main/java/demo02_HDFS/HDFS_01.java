package demo02_HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 21:02
 * @Description:
 */
public class HDFS_01 {

    public static void main(String[] args) throws Exception {


        //create();
        putFile();

    }

    /**
     * 创建一个文件,并往里面写入数据
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void create() throws URISyntaxException, IOException, InterruptedException {


        //实例化configuration对象
        Configuration configuration = new Configuration();

        //实例化文件系统对象hdfs
        FileSystem fileSystem =  FileSystem.get(new URI("hdfs://192.168.100.100"),configuration,"root");

        //创建一个文件
        FSDataOutputStream outputStream = fileSystem.create(new Path("/abcba/abcba.txt"));

        //确定需要写入的数据
        byte[] bytes = "Nothing is possible!".getBytes();

        //写入数据
        outputStream.write(bytes,0,bytes.length);

        //关闭资源
        outputStream.close();


    }

    /**
     * 将本地数据上传到 HDFS 的文件夹下
     * @throws Exception
     */
    public static void putFile() throws Exception {

        //实例化configuration对象
        Configuration configuration = new Configuration();

        //实例化文件系统对象hdfs

        FileSystem fileSystem =  FileSystem.get(new URI("hdfs://192.168.100.100"),configuration,"root");


        fileSystem.copyFromLocalFile(new Path("E:\\2019大数据课程\\DeBug\\测试\\order\\素材\\4\\map端join\\input\\orders.txt"),new Path("/aa"));

        System.out.println("复制完成!!");


    }
}