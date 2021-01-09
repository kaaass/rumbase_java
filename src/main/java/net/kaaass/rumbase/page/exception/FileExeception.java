package net.kaaass.rumbase.page.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E9001 文件异常
 * <p>
 * E9001-1  创建文件失败
 * E9001-2  写入文件失败
 * E9001-3  文件打开失败
 * E9001-4  游标越界
 *
 * @author XuanLaoYee
 */
public class FileExeception extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<Integer, String>(){{
        put(1, "创建文件失败");
        put(2, "写入文件失败");
        put(3, "文件打开失败");
        put(4, "offset越界");
    }};

    /**
     * 文件异常
     *
     * @param subId  子错误号
     */
    public FileExeception(int subId){
        super(9001, subId, REASONS.get(subId));
    }
}
