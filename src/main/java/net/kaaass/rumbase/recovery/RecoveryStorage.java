package net.kaaass.rumbase.recovery;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.io.JBBPOut;
import com.igormaznitsa.jbbp.mapper.Bin;
import net.kaaass.rumbase.dataitem.IItemStorage;
import net.kaaass.rumbase.dataitem.ItemManager;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.mock.MockTransactionContext;

import java.io.IOException;
import java.util.List;

/**
 *  日志记录相关内容
 *
 * @author kaito
 */
public class RecoveryStorage implements  IRecoveryStorage {

    private IItemStorage itemStorage;

    public RecoveryStorage(IItemStorage itemStorage) {
        this.itemStorage = itemStorage;
    }

    /**
     * 读取日志文件并且解析得到日志管理器
     * @param fileName
     * @return 日志管理器
     */
    public static IRecoveryStorage ofFile(String fileName) throws FileException, IOException, LogException, PageException {
        var itemStorage = ItemManager.fromFileWithoutLog(fileName + ".log");
        return new RecoveryStorage(itemStorage);
    }

    /**
     * 创建日志文件
     * @param fileName 文件名
     * @return
     */
    public static IRecoveryStorage ofNewFile(String fileName) throws FileException, IOException, PageException, LogException {
        var metadata = JBBPOut.BeginBin().Int(HEADER).End().toByteArray();
        var itemStorage = ItemManager.createFileWithoutLog(fileName + ".log",metadata);
        return new RecoveryStorage(itemStorage);
    }

    @Override
    public void begin(int xid, List<Integer> snapshots) throws IOException, FileException {
        var jbbp = JBBPOut.BeginBin().
                String(TX_BEGIN).
                Int(xid).
                Int(snapshots.size());
        for ( var i : snapshots){
            jbbp = jbbp.Int(i);
        }
        var bytes = jbbp.End().toByteArray();
        var uuid = itemStorage.insertItemWithoutLog(bytes);
        itemStorage.flush(uuid);
    }

    @Override
    public void rollback(int xid) throws IOException, FileException {
        var bytes = JBBPOut.BeginBin().
                String(TX_ABORT).
                Int(xid).
                End().toByteArray();
        var uuid = itemStorage.insertItemWithoutLog(bytes);
        itemStorage.flush(uuid);
    }

    @Override
    public void commit(int xid) throws IOException, FileException {
        var bytes = JBBPOut.BeginBin().
                String(TX_COMMIT).
                Int(xid).
                End().toByteArray();
        var uuid = itemStorage.insertItemWithoutLog(bytes);
        itemStorage.flush(uuid);
    }

    @Override
    public void insert(int xid, long uuid, byte[] item) throws IOException, FileException {
        var bytes = JBBPOut.BeginBin().
                String(INSERT_FLAG).
                Int(xid).
                Long(uuid).
                Int(item.length).
                Byte(item).
                End().toByteArray();
        var id = itemStorage.insertItemWithoutLog(bytes);
        itemStorage.flush(id);

    }

    @Override
    public void update(int xid, long uuid, byte[] item_before,byte[] item_after) throws IOException, FileException {
        var bytes = JBBPOut.BeginBin().
                String(UPDATE_FLAG).
                Int(xid).
                Long(uuid).
                Int(item_before.length).
                Byte(item_before).
                Int(item_after.length).
                Byte(item_after).
                End().toByteArray();
        var id = itemStorage.insertItemWithoutLog(bytes);
        itemStorage.flush(id);
    }

    @Override
    public void updateMeta(int xid, long metaUUID) throws IOException, FileException {
        var bytes  =JBBPOut.BeginBin().
                String(METADATA_UPDATE_FLAG).
                Int(xid).
                Long(metaUUID).
                End().toByteArray();
        var uuid = itemStorage.insertItemWithoutLog(bytes);
        itemStorage.flush(uuid);

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

    /**
     * 日志标志
     */
    final private static int HEADER = 4567;

    final private static String INSERT_FLAG = "i";

    final private static String TX_BEGIN = "b";

    final private static String TX_ABORT = "a";

    final private static String TX_COMMIT = "c";

    final private static String UPDATE_FLAG = "u";

    final private static String METADATA_UPDATE_FLAG = "m";




}
