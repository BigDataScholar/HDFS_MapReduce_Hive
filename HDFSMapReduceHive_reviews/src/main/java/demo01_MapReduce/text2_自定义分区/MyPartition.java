package demo01_MapReduce.text2_自定义分区;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 09:04
 * @Description:
 */
public class MyPartition extends Partitioner<Text, NullWritable> {


    /**
     *   分区逻辑,注意【分区默认是从0开始】
     * @param text
     * @param nullWritable
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {

        //将text类型的数据转换成string类型的数据
        String str = text.toString();

        String[] split = str.split("\t");

        int num = Integer.parseInt(split[5]);

        //把num大于15的放在1号分区
        if (num > 15) {

            return 1;

        }else{


            //小于等于15的放在0号分区
            return 0;

        }


    }
}