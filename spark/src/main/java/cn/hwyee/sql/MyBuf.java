package cn.hwyee.sql;

import lombok.Data;

import java.io.Serializable;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName MyBuf
 * @description 必须是公共类
 * @date 2024/7/1
 * @since JDK 1.8
 */
@Data
public class MyBuf implements Serializable {
    private long sum;
    private long count;

}
