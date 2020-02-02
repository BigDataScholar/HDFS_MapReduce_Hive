package demo02_HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 15:47
 * @Description:
 */
public class HDFS_02 {

    public static void main(String[] args) throws IOException, URISyntaxException {


        //调用方法
        writeFilePath();
    }


    public static void writeFilePath() throws URISyntaxException, IOException {

        //1.实例化配置文件对象
        Configuration configuration = new Configuration();

        //2.实例化文件系统对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.100.100"), configuration);

        //3.创建文件夹
        fileSystem.mkdirs(new Path("/cc/datas"));

        //4.获取一个目录下的所有文件【列表】
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/cc/DataFlow"));

        //5.创建append追加流
        //FSDataOutputStream outputStream = fileSystem.create(new Path("/cc/datas/tdata.txt"));


        /**   同时创建文件和追加文件将报异常AlreadyBeingCreatedException
         *    具体的解决方案为:  在create之后及时将FSDataOutputStream关闭。
         */

        FSDataOutputStream append = fileSystem.append(new Path("/cc/datas/tdata.txt"));

        //遍历文件名数组,找到合适的放入到文件中
        for (FileStatus fileStatus : fileStatuses) {

            String path = fileStatus.getPath().toString();

            // path 数据格式:  hdfs://192.168.100.100/cc/DataFlow/part-r-00000
            if (path.contains("part")){

                //直接写入字符串
                append.writeBytes(path);

            }
        }

        //关闭资源
        append.close();


    }
}