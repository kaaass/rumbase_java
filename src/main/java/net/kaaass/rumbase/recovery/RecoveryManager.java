package net.kaaass.rumbase.recovery;

import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.recovery.mock.MockRecoveryStorage;

import java.io.IOException;

/**
 * 日志恢复的管理器，用来对每个数据库文件进行恢复
 *
 * @author kaito
 */
public class RecoveryManager {
    /**
     * 对某个数据库文件进行恢复
     *
     * @param fileName 文件名
     * @return 数据库日志管理器
     */
    public static void recovery(String fileName) throws PageException, LogException, FileException, IOException {

        RecoveryStorage.ofFile(fileName).recovery();
    }

    public static IRecoveryStorage getRecoveryStorage(String fileName) throws FileException, IOException, LogException, PageException {
        return RecoveryStorage.ofFile(fileName);
    }

    public static IRecoveryStorage createRecoveryStorage(String fileName) throws IOException, FileException, PageException, LogException {
        return RecoveryStorage.ofNewFile(fileName);
    }
}
