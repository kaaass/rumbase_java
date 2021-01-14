package net.kaaass.rumbase.recovery;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.io.JBBPOut;
import com.igormaznitsa.jbbp.mapper.Bin;
import net.kaaass.rumbase.dataitem.ItemManager;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.exception.LogException;

import java.io.IOException;
import java.util.List;

/**
 *  日志记录相关内容
 *
 * @author kaito
 */
public class RecoveryStorage implements  IRecoveryStorage {

    /**
     * 读取日志文件并且解析得到日志管理器
     * @param fileName
     * @return 日志管理器
     */
    public static IRecoveryStorage ofFile(String fileName) throws FileException, IOException, LogException {
        return new RecoveryStorage();
    }

    /**
     * 创建日志文件
     * @param fileName 文件名
     * @return
     */
    public static IRecoveryStorage ofNewFile(String fileName) throws FileException, IOException, PageException {
        return new RecoveryStorage();
    }

    @Override
    public void begin(int xid, List<Integer> snapshots) {

    }

    @Override
    public void rollback(int xid) {

    }

    @Override
    public void commit(int xid) {

    }

    @Override
    public void insert(int xid, long uuid, byte[] item) {

    }

    @Override
    public void update(int xid, long uuid, byte[] item) {

    }

    @Override
    public void updateMeta(int xid, long metaUUID) {

    }

    @Override
    public List<byte[]> getContent() {
        return null;
    }

    /**
     * 日志头信息
     * TODO: 记录点等特殊要求
     */
    public static class LogHeader{
        @Bin
        int header;
    }
    final private static int HEADER = 1234;

}
