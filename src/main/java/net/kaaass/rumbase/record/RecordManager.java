package net.kaaass.rumbase.record;

import net.kaaass.rumbase.record.mock.MockRecordStorage;

/**
 * 记录管理类
 *
 * @author kaaass
 */
public class RecordManager {


    /**
     * 从文件获取记录存储对象
     *
     * @param filepath 记录文件
     * @return 记录存储对象
     */
    public static IRecordStorage fromFile(String filepath) {
        return MockRecordStorage.ofFile(filepath);
    }
}
