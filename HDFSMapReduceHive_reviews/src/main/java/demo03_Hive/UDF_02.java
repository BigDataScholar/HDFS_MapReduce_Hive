package demo03_Hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 20:53
 * @Description:
 */
public class UDF_02 extends UDF {


    public Text evaluate(Text t1,Text t2){


        int num = Integer.parseInt(t2 + "")+80;


        String string = t1.toString();


        return new Text(string+"feng-"+num);


    }
}