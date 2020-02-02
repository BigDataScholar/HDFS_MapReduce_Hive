package demo01_MapReduce.text1_BrowserCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/24 18:02
 * @Description:
 */
public class Browser implements Writable {


    //浏览器
    private String export;

    //IP
    private Integer ip;


    @Override
    public String toString() {
        return "Browser{" +
                "export='" + export + '\'' +
                ", ip=" + ip +
                '}';
    }

    public Browser(String export, Integer ip) {
        this.export = export;
        this.ip = ip;
    }

    public Browser() {
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(export);
        out.writeInt(ip);
    }

    public String getExport() {
        return export;
    }

    public void setExport(String export) {
        this.export = export;
    }

    public Integer getIp() {
        return ip;
    }

    public void setIp(Integer ip) {
        this.ip = ip;
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        in.readUTF();
        in.readInt();
    }
}