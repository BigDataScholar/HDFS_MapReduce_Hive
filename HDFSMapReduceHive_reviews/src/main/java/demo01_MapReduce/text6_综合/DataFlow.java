package demo01_MapReduce.text6_综合;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 09:39
 * @Description:
 */
public class DataFlow implements Writable {


    //上传流量
    private Integer upFlow;

    //下载流量
    private Integer downFlow;

    //上传总流量
    private Integer upCountFlow;

    //下载总流浪
    private Integer downCountFlow;


    @Override
    public String toString() {
        return "DataFlow{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", upCountFlow=" + upCountFlow +
                ", downCountFlow=" + downCountFlow +
                '}';
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    public DataFlow() {
    }

    public DataFlow(Integer upFlow, Integer downFlow, Integer upCountFlow, Integer downCountFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.upCountFlow = upCountFlow;
        this.downCountFlow = downCountFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {



        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upCountFlow);
        dataOutput.writeInt(downCountFlow);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {


       this.upFlow =  dataInput.readInt();
       this.downFlow = dataInput.readInt();
       this.upCountFlow = dataInput.readInt();
       this.downCountFlow = dataInput.readInt();

    }
}