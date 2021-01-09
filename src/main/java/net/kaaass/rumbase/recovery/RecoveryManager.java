package net.kaaass.rumbase.recovery;

import net.kaaass.rumbase.recovery.mock.MockRecoveryStorage;

/**
 * 日志恢复的管理器，用来对每个数据库文件进行恢复
 * @author kaito
 */
public class RecoveryManager {
    /**
     * 对某个数据库文件进行恢复，并且返回对应的日志管理器
     *
     * @param fileName 文件名
     * @return 数据库日志管理器
     */
    public static IRecoveryStorage recovery(String fileName) {
        // TODO:对数据进行恢复
        return new MockRecoveryStorage();
    }
}
