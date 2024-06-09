package bigdata.hive.udf;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName LengthDemo
 * @description 校验string长度
 * @date 2024/6/9
 * @since JDK 1.8
 */
@Slf4j
public class LengthDemo extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("LengthDemo only takes one argument");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("LengthDemo only takes primitive types, got " + arguments[0].getTypeName());
        }
        log.info("arguments[0] = " + arguments[0]);
        log.info("arguments[0].getCategory() = " + arguments[0].getCategory());
        log.info("arguments[0].getTypeName() = " + arguments[0].getTypeName());
        PrimitiveObjectInspector poi =  (PrimitiveObjectInspector)arguments[0];
        if (poi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("LengthDemo only takes string type, got " + poi.getTypeName());
        }
        //返回函数返回值的Inspector 供之后的方法使用
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * evaluate:
     * 每行都会调用这个方法
     * @author hui
     * @version 1.0
     * @param arguments
     * @return java.lang.Object
     * @date 2024/6/9 21:56
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] != null) {
            DeferredObject o = arguments[0];
            String str = o.toString();
            return str.length();
        }
        return null;
    }

    /**
     * getDisplayString:
     * 输入explain plan时，会调用该方法
     * @author hui
     * @version 1.0
     * @param children
     * @return java.lang.String
     * @date 2024/6/9 21:56
     */
    @Override
    public String getDisplayString(String[] children) {
        return "LengthDemo";
    }
}
