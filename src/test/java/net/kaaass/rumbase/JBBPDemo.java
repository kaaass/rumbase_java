package net.kaaass.rumbase;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
import com.igormaznitsa.jbbp.io.JBBPOut;
import com.igormaznitsa.jbbp.model.JBBPFieldArrayByte;
import com.igormaznitsa.jbbp.model.JBBPFieldShort;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * JBBP库的使用示例
 */
@Slf4j
public class JBBPDemo {

    /**
     * 解析字节流
     */
    @Test
    public void parseFromStream() throws IOException {
        // 首次解析
        var parser = JBBPParser.prepare(
                "ushort shortVal;" +
                        "int intVal;" +
                        "byte[4] bytes;"
        );
        var stream = new ByteArrayInputStream(
                new byte[]{0, 12,
                        (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                        'a', 'b', 'c', 'd',
                        66, 0, 12});
        var result = parser.parse(stream);
        Arrays.stream(result.getArray()).forEach(field ->
                log.info("解析字段: {}", field.getFieldPath()));
        log.info("bytes = {}",
                result
                        .findFieldForNameAndType("bytes", JBBPFieldArrayByte.class)
                        .getArray());
        // 再次解析
        parser = JBBPParser.prepare(
                "byte byteVal;" +
                        "short shortVal;"
        );
        result = parser.parse(stream);
        log.info("shortVal = {}",
                result
                        .findFieldForNameAndType("shortVal", JBBPFieldShort.class)
                        .getAsInt());
        //
        stream.close();
    }

    /**
     * 不编写脚本进行解析
     */
    @Test
    public void parseWithoutScript() throws IOException {
        var data = new byte[]{1, 2, 0, 4, 0, 4, 5, 'c', 'h', -28, -72, -83};
        var stream = new JBBPBitInputStream(new ByteArrayInputStream(data));
        // 解析过程就是读入一个个字段
        log.info("读入short {}", stream.readUnsignedShort(JBBPByteOrder.LITTLE_ENDIAN));
        log.info("读入数组 {}", stream.readShortArray(2, JBBPByteOrder.BIG_ENDIAN));
        log.info("读入字符串 {}", stream.readString(JBBPByteOrder.BIG_ENDIAN));
    }

    /**
     * 把数据写到字符串数组
     */
    @Test
    public void writeToBinary() throws IOException {
        var result = JBBPOut.BeginBin().
                Bit(1, 0, 0, 1, 1, 1). // 0b111001
                Short(233).
                String("汉字").
                End().toByteArray();
        log.info("{}", result);
    }
}
