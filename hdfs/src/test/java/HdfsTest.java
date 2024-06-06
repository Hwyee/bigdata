import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName HdfsTest
 * @description
 * @date 2024/5/29
 * @since JDK 1.8
 */

public class HdfsTest {

    private static FileSystem fs ;
    @BeforeAll
    public static void connect() throws URISyntaxException, IOException, InterruptedException {
        URI url = new URI("hdfs://hadoop001:8020");
        //配置文件优先级：1.代码中配置 2.代码资源目录配置文件 3.hdfs.site.xml 4.默认配置文件 hdfs-default.xml
        Configuration configuration = new Configuration();
        fs = FileSystem.get(url, configuration,"bigdata");
    }
    @Test
    public void DirTest() throws IOException {
        if (fs.exists(new Path("/javatest"))) {
            fs.mkdirs(new Path("/javatest"));
        }
    }

    @Test
    public void upload() throws IOException {
        fs.copyFromLocalFile(false,true,new Path("./build.gradle"),new Path("/javatest/build.gradle"));
    }

    @AfterAll
    public static void close() throws IOException {
        fs.close();
    }
}
