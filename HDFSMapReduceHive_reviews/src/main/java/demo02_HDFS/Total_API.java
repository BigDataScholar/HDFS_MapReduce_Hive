package demo02_HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/24 21:51
 * @Description:
 */
public class Total_API {

    static Configuration configuration = new Configuration();

    static FileSystem hdfs;

     static {
        try {
            hdfs = FileSystem.get(new URI("hdfs://192.168.100.100"),configuration,"root");
        } catch (Exception e) {
            e.printStackTrace();

        }
    }


    public static void main(String[] args) throws Exception {


        //listStatus();
        //rename();
        // GetTime();
        //mkdir();
        //addFile();
        //put();
        check();
    }

    /**
     *  遍历一个目录下所有文件的路径
     * @throws Exception
     */
    private static void listStatus() throws Exception {

        //获取某一个目录下的所有文件
        FileStatus[] fileStatuses = hdfs.listStatus(new Path("/"));

        //遍历打印输出
        for (FileStatus fileStatus : fileStatuses) {

            System.out.println(fileStatus.getPath().toString());

        }

        //关闭资源
        hdfs.close();

    }

    /**
     * 修改文件的名字
     */
    private static void rename()throws Exception{


        //根据新旧两个文件的路径创建两个path对象
        Path path1 = new Path("/exports");
        Path path2 = new Path("/export");

        boolean rename = hdfs.rename(path1, path2);
        String result = rename?"修改成功":"修改失败!";

        System.out.println(result);

    }
    /**
     *  获取文件的修改时间(毫秒值)
     * @throws Exception
     */
    private static void GetTime() throws Exception{

        FileStatus fileStatus = hdfs.getFileStatus(new Path("/idea.txt"));

        long modificationTime = fileStatus.getModificationTime();

        System.out.println("文件的修改时间为:"+modificationTime);

    }

    /**
     * 删除文件
     * @throws Exception
     */
    private static void deletefile() throws Exception{

        boolean delete = hdfs.delete(new Path("1125.txt"), true);

        System.out.println("Delete?"+delete);

    }

    /**
     * 创建文件夹
     */

    private static void mkdir() throws Exception{

        boolean mkdirs = hdfs.mkdirs(new Path("/dd"));

        System.out.println(mkdirs?"创建成功!":"创建失败!");

    }

    /**
     * 创建文件并写入内容【重点】
     */
    private static void addFile() throws IOException {

        //确定需要写入的内容,并转换成字节数组
        byte[] bytes = "hello world!my hadoop and hdfs and mapreduce!".getBytes();

        //创建文件
        FSDataOutputStream outputStream = hdfs.create(new Path("/dd/1125.txt"));
        //写入数据
        outputStream.write(bytes,0,bytes.length);

        System.out.println("写入成功!");
        //关闭输出流
        outputStream.close();



    }

    /**
     * 上传数据【windows--->linux】
     */
    private static void put() throws Exception{

        //原文件路径
        Path path1 = new Path("G:\\Python\\test.csv");

        //目标路径
        Path path2 = new Path("/");


        hdfs.copyFromLocalFile(path1,path2);

        System.out.println("上传成功!!");

    }

    private static void check() throws Exception{

        Path path = new Path("/aa");

        boolean exists = hdfs.exists(path);


        System.out.println(exists?"该文件存在!":"该文件不存在!");


    }

}