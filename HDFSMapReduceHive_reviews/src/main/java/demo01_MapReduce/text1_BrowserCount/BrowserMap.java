package demo01_MapReduce.text1_BrowserCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/24 18:01
 * @Description:
 */
public class BrowserMap extends Mapper<LongWritable, Text,Text,Browser> {



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //设置一个全局对象
       Browser browser = new Browser();

        //将text类型的数据转换成string类型
        String string = value.toString();

        //判空
        if (!string.equals("")) {

            String[] s = string.split(" ");

            //获取到浏览器
            String exports = s[11];

            //获取到状态码
            String sta = s[8];

                if (!exports.equals("")&&!sta.equals("")) {

                    //将字符串的数据转换成int类型进行比较
                    int status = Integer.parseInt(sta);


                    if (status > 0 && status < 400 && !exports.contains("-")) {

                        browser.setExport(exports);
                        browser.setIp(status);

                        //输出数据
                        context.write(new Text(exports.substring(1) + ""), browser);

                    }

                }
            }


    }
}