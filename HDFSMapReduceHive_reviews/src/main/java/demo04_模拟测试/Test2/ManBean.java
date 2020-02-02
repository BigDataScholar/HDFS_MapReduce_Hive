package demo04_模拟测试.Test2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/26 20:09
 * @Description:
 */
public class ManBean implements Writable {

    //姓名
    private String name;

    //编号
    private String id;

    //性别
    private String sex;

    //年龄
    private String age;


    @Override
    public String toString() {
        return "ManBean{" +
                "name='" + name + '\'' +
                ", id='" + id + '\'' +
                ", sex='" + sex + '\'' +
                ", age='" + age + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public ManBean() {
    }

    public ManBean(String name, String id, String sex, String age) {
        this.name = name;
        this.id = id;
        this.sex = sex;
        this.age = age;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {


        dataOutput.writeUTF(name+"");
        dataOutput.writeUTF(id+"");
        dataOutput.writeUTF(sex+"");
        dataOutput.writeUTF(age+"");

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.name = dataInput.readUTF();
        this.id = dataInput.readUTF();
        this.sex = dataInput.readUTF();
        this.age = dataInput.readUTF();

    }
}