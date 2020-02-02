package demo01_MapReduce.text4_MapJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/25 17:59
 * @Description:
 */
public class JoinBean implements Writable {

    //定义属性
    private String id;

    private String date;

    private String pid;

    private String amount;

    private String pname;

    private String category_id;

    private String price;



    @Override
    public String toString() {
        return "JoinBean{" +
                "id='" + id + '\'' +
                ", date='" + date + '\'' +
                ", pid='" + pid + '\'' +
                ", amount='" + amount + '\'' +
                ", pname='" + pname + '\'' +
                ", category_id='" + category_id + '\'' +
                ", price='" + price + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public JoinBean() {
    }

    public JoinBean(String id, String date, String pid, String amount, String pname, String category_id, String price) {
        this.id = id;
        this.date = date;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
        this.category_id = category_id;
        this.price = price;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {

          dataOutput.writeUTF(id+"");
          dataOutput.writeUTF(date+"");
          dataOutput.writeUTF(pid+"");
          dataOutput.writeUTF(amount+"");
          dataOutput.writeUTF(pname+"");
          dataOutput.writeUTF(category_id+"");
          dataOutput.writeUTF(price+"");

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.id=dataInput.readUTF();
        this.date=dataInput.readUTF();
        this.pid=dataInput.readUTF();
        this.amount=dataInput.readUTF();
        this.pname=dataInput.readUTF();
        this.category_id=dataInput.readUTF();
        this.price=dataInput.readUTF();


    }
}