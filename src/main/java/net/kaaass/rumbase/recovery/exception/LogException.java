package net.kaaass.rumbase.recovery.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

public class LogException extends RumbaseException {
    /**
     * 构造Rumbase异常
     *
     */

    public static final Map<Integer, String> REASONS = new HashMap<Integer, String>() {{
        put(1, "日志文件无法解析");
        put(2, "事务ID不存在");
        put(3, "事务提交日志写回错误");
        put(4, "事务回滚日志写回错误");
        put(5, "事务提交日志写回错误");
        put(6, "插入数据项日志写回错误");
        put(7, "更新数据项日志写回错误");
        put(8, "更新头信息日志写回错误");
        put(9, "日志文件创建失败");
        put(10, "日志文件打开失败");
        put(11, "日志获取原文件失败");
    }};

    public LogException(int subId) {
        super(3001,subId,REASONS.get(subId));
    }
}
