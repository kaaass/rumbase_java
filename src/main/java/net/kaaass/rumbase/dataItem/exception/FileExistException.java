package net.kaaass.rumbase.dataItem.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

public class FileExistException extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<>(){{
        put(1, "要创建的文件已存在");
        put(2, "查找的文件不存在");
    }};

    public FileExistException(int subID) {
        super(6001,subID,REASONS.get(subID));
    }
}
