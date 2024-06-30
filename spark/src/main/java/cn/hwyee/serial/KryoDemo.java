package cn.hwyee.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName KryoDemo
 * @description
 * @date 2024/6/24
 * @since JDK 1.8
 */
@Slf4j
public class KryoDemo {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        KryoDemo kyroDemo = new KryoDemo();
        kyroDemo.kyroSerial();
        kyroDemo.javaSerial();
    }

    public void javaSerial() throws IOException, ClassNotFoundException {
        Person object = new Person("java",10);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("spark/java.bin"));
        objectOutputStream.writeObject(object);//88字节
        objectOutputStream.close();
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("spark/java.bin"));
        Person object2 = (Person)objectInputStream.readObject();
        objectInputStream.close();
        System.out.println(object2);
    }

    public void kyroSerial() throws FileNotFoundException {
        Kryo kryo = new Kryo();
        kryo.register(Person.class);

        Person object = new Person("kyro",10);

        Output output = new Output(new FileOutputStream("spark/file.bin"));
        kryo.writeObject(output, object);//5字节
        output.close();

        Input input = new Input(new FileInputStream("spark/file.bin"));
        Person object2 = kryo.readObject(input, Person.class);
        System.out.println(object2);
        input.close();
    }
}

class Person implements Serializable {
    private String name;
    private int age;
    public Person() {
    }
    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

}