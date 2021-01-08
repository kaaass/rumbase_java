package net.kaaass.rumbase.dataItem.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

public class UUIDException extends RumbaseException {

    public static final Map<Integer, String> REASONS = new HashMap<>(){{
        put(1, "要插入的UUID已存在");
        put(2,"要查找的UUID不存在");
    }};

    public UUIDException(int subID) {
        super(7001,subID,REASONS.get(subID));
    }
}
