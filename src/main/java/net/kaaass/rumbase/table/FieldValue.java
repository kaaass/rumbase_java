package net.kaaass.rumbase.table;

import com.igormaznitsa.jbbp.io.JBBPOut;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.io.IOException;

/**
 * FieldValue是一个元组的属性的包装
 *
 * @param <T> 属性的类型
 */
@RequiredArgsConstructor
public class FieldValue<T> implements Comparable<FieldValue> {

    /**
     * 属性的值
     * <p>
     * fixme 检查VARCHAR长度不能超过 2^15 - 1
     */
    @Getter
    @NonNull
    private final T value;

    /**
     * 属性的类型枚举
     */
    @Getter
    @NonNull
    private final FieldType type;

    /**
     * 获取当前FieldValue所占字节数
     *
     * @return 字节数
     */
    public int getLen() {
        if (type == FieldType.FLOAT)
            return 4;
        else if (type == FieldType.INT)
            return 4;
        else if (type == FieldType.VARCHAR)
            return 2 + ((char[]) value).length;
        return -1;
    }

    /**
     * 比较两个FieldValue大小
     *
     * @param fieldValue 另一个大小
     * @return -1 0 1 分别代表 this < = > 另一个
     */
    @Override
    public int compareTo(FieldValue fieldValue) {
        var res = 0;

        if (fieldValue.getType() != type)
            return 0;

        switch (type) {
            case INT: {
                var val1 = (int) this.getValue();
                var val2 = (int) fieldValue.getValue();

                res = val1 - val2;
            }
            case FLOAT: {

                var tmp = (float) this.getValue() - (float) fieldValue.getValue();

                if (tmp < 0)
                    res = -1;
                else if (tmp > 0)
                    res = 1;
            }
            case VARCHAR: {

                var str1 = new String((char[]) this.getValue());
                var str2 = new String((char[]) fieldValue.getValue());

                res = str1.compareTo(str2);
            }
        }

        return res;
    }

    /**
     * 将自身的值追加到JBBPOut上
     *
     * @param jbbpOut 待追加的JBBPOut
     * @throws IOException 追加失败
     */
    public void append2JBBPOut(JBBPOut jbbpOut) throws IOException {

        switch (type) {
            case INT: {
                jbbpOut.Bin((int) value);
            }
            case FLOAT: {
                jbbpOut.Bin((float) value);
            }
            case VARCHAR: {
                var charArray = (char[]) value;
                jbbpOut.Bin((short) charArray.length).Bin(charArray);
            }
        }
    }
}
