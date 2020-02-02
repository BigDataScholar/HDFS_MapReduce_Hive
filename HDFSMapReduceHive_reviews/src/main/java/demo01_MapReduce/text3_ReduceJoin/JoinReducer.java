package demo01_MapReduce.text3_ReduceJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 18:21
 * @Description:
 */
public class JoinReducer extends Reducer<Text,JoinBean,JoinBean, NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {


        //实例化一个对象
        JoinBean joinBean = new JoinBean();

        for (JoinBean joinBean1 : values) {

            //如果遍历的对象pid值不为null,则为新实例化的对象赋值
            if (joinBean1.getId()!=null&&!joinBean1.getId().equals("null")){


                joinBean.setId(joinBean1.getId());
                joinBean.setDate(joinBean1.getDate());
                joinBean.setPid(joinBean1.getPid());
                joinBean.setAmount(joinBean1.getAmount());

            }else {

                joinBean.setPname(joinBean1.getPname());
                joinBean.setCategory_id(joinBean1.getCategory_id());
                joinBean.setPrice(joinBean1.getPrice());

            }
        }


        //将最后的进行输出
        context.write(joinBean,NullWritable.get());

    }
}