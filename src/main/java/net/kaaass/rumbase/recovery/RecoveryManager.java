package net.kaaass.rumbase.recovery;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.recovery.mock.MockRecoveryStorage;

import java.io.IOException;

/**
 * 日志恢复的管理器，用来对每个数据库文件进行恢复
 * FIXME 需要同时在所有文件记录事务的发生
 *
 * @author kaito
 */
@Slf4j
public class RecoveryManager {
    /**
     * 对某个数据库文件进行恢复
     *
     * @param fileName 文件名
     * @return 数据库日志管理器
     */
    public static void recovery(String fileName) throws LogException {
        IRecoveryStorage recoveryStorage;
        try {
            recoveryStorage = RecoveryStorage.ofFile(fileName);
        } catch (LogException e) {
            log.debug("恢复日志不存在，忽略该文件的恢复", e);
            return;
        }
        recoveryStorage.recovery();
    }

    public static IRecoveryStorage getRecoveryStorage(String fileName) throws LogException {
        return RecoveryStorage.ofFile(fileName);
    }

    public static IRecoveryStorage createRecoveryStorage(String fileName) throws  LogException {
        return RecoveryStorage.ofNewFile(fileName);
    }
}
