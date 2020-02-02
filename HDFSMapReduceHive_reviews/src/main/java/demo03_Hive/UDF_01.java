package demo03_Hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 17:23
 * @Description:
 */
public class UDF_01 extends UDF {



    public Text evaluate(Text text){


        if (text == null){

            return null;
        }

        String str = text.toString();

        int i = Integer.parseInt(str);

        int rei = i * 3;

        //返回计算后的结果
        return new Text(rei+"");

    }
}