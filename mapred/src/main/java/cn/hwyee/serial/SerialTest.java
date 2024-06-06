package cn.hwyee.serial;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName SerialTest
 * @description 如果key或value是自己定义的bean对象，则需要序列化
 * @date 2024/5/30
 * @since JDK 1.8
 */
public class SerialTest implements Writable {
    int age;

    String name;

    String address;

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(age);
        if (name == null){
            name = "";
        }
        if (address == null){
            address = "";
        }
        out.writeUTF(name);
        out.writeUTF(address);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //反序列化的顺序需要和序列化一致
        this.age = in.readInt();
        this.name = in.readUTF();
        this.address = in.readUTF();
    }
}
