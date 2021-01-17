package net.kaaass.rumbase.recovery;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.io.JBBPOut;
import com.igormaznitsa.jbbp.mapper.Bin;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.IItemStorage;
import net.kaaass.rumbase.dataitem.ItemManager;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.exception.LogException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 日志记录相关内容
 *
 * @author kaito
 */
@Slf4j
public class RecoveryStorage implements IRecoveryStorage {


    /**
     * 用于对自己的日志记录进行管理
     */
    private IItemStorage logStorage;
    /**
     * 要恢复的对象对应的文件名
     */
    private String fileName;

    public RecoveryStorage(IItemStorage itemStorage, String fileName) {
        this.logStorage = itemStorage;
        this.fileName = fileName;
    }

    /**
     * 读取日志文件并且解析得到日志管理器
     *
     * @param fileName
     * @return 日志管理器
     */
    public static IRecoveryStorage ofFile(String fileName) throws LogException {
        try {
            var itemStorage = ItemManager.fromFileWithoutLog(fileName + ".log");
            return new RecoveryStorage(itemStorage, fileName);
        } catch (FileException | PageException e) {
            throw new LogException(10);
        }
    }

    /**
     * 创建日志文件
     *
     * @param fileName 文件名
     * @return
     */
    public static IRecoveryStorage ofNewFile(String fileName) throws LogException {
        try {
            var metadata = JBBPOut.BeginBin().Int(HEADER).End().toByteArray();
            var itemStorage = ItemManager.createFileWithoutLog(fileName + ".log", metadata);
            return new RecoveryStorage(itemStorage, fileName);
        } catch (IOException | FileException | PageException e) {
            throw new LogException(9, e);
        }
    }

    @Override
    public void begin(int xid, List<Integer> snapshots) throws LogException {
        try {
            var jbbp = JBBPOut.BeginBin().
                    Byte(TX_BEGIN).
                    Int(xid).
                    Int(snapshots.size());
            for (var i : snapshots) {
                jbbp = jbbp.Int(i);
            }
            var bytes = jbbp.End().toByteArray();
            var uuid = logStorage.insertItemWithoutLog(bytes);
            logStorage.flush(uuid);
        } catch (IOException | FileException e) {
            throw new LogException(3);
        }
    }

    @Override
    public void rollback(int xid) throws LogException {
        try {
            var bytes = JBBPOut.BeginBin().
                    Byte(TX_ABORT).
                    Int(xid).
                    End().toByteArray();
            var uuid = logStorage.insertItemWithoutLog(bytes);
            logStorage.flush(uuid);
        } catch (FileException | IOException e) {
            throw new LogException(4);
        }
    }

    @Override
    public void commit(int xid) throws LogException {
        try {
            var bytes = JBBPOut.BeginBin().
                    Byte(TX_COMMIT).
                    Int(xid).
                    End().toByteArray();
            var uuid = logStorage.insertItemWithoutLog(bytes);
            logStorage.flush(uuid);
        } catch (FileException | IOException e) {
            throw new LogException(5);
        }

    }

    @Override
    public void insert(int xid, long uuid, byte[] item) throws LogException {
        try {
            var bytes = JBBPOut.BeginBin().
                    Byte(INSERT_FLAG).
                    Int(xid).
                    Long(uuid).
                    Int(item.length).
                    Byte(item).
                    End().toByteArray();
            var id = logStorage.insertItemWithoutLog(bytes);
            logStorage.flush(id);
        } catch (FileException | IOException e) {
            throw new LogException(6);
        }
    }

    @Override
    public void update(int xid, long uuid, byte[] itemBefore, byte[] itemAfter) throws LogException {
        try {
            var bytes = JBBPOut.BeginBin().
                    Byte(UPDATE_FLAG).
                    Int(xid).
                    Long(uuid).
                    Int(itemBefore.length).
                    Byte(itemBefore).
                    Int(itemAfter.length).
                    Byte(itemAfter).
                    End().toByteArray();
            var id = logStorage.insertItemWithoutLog(bytes);
            logStorage.flush(id);
        } catch (FileException | IOException e) {
            throw new LogException(7);
        }
    }

    @Override
    public void updateMeta(int xid, long beforeUuid, byte[] metadata) throws LogException {
        try {
            var bytes = JBBPOut.BeginBin().
                    Byte(METADATA_UPDATE_FLAG).
                    Int(xid).
                    Long(beforeUuid).
                    Int(metadata.length).
                    Byte(metadata).
                    End().toByteArray();
            var uuid = logStorage.insertItemWithoutLog(bytes);
            logStorage.flush(uuid);
        } catch (FileException | IOException e) {
            throw new LogException(8);
        }

    }

    @Override
    public List<byte[]> getContent() {
        List<byte[]> logList = new ArrayList<>();
        var maxPageId = logStorage.getMaxPageId();
        for (int i = 1; i <= maxPageId; i++) {
            var logs = logStorage.listItemByPageId(i);
            for (var l : logs) {
                if (l.length > 4) {
                    logList.add(l);
                }
            }
        }
        return logList;
    }

    @Override
    public void recovery() throws LogException {
        try {
            var itemStorage = ItemManager.fromFile(this.fileName);
            // TODO 判断是否进行恢复
            log.info("开始恢复文件 {}...", this.fileName);
            var logs = getContent();
            var xidMaps = parseXid(logs);
            log.debug("文件 {} 回滚事务：{}", this.fileName, xidMaps);
            // 逐条回滚
            for (var log : logs) {
                parseLog(log, itemStorage, xidMaps);
            }
        } catch (FileException | PageException e) {
            throw new LogException(11);
        }
    }

    /**
     * 先进行一遍解析，得到所有已完成的事务和未完成的事务，来决定redo还是undo
     */
    private Map<Character, List<Integer>> parseXid(List<byte[]> logs) throws LogException {
        List<Integer> commitXids = List.of(0);
        List<Integer> abortXids = new ArrayList<>();

        for (var binLog : logs) {
            Tx tx;
            try {
                tx = parseTx(binLog);
            } catch (IOException e) {
                throw new LogException(1);
            }
            switch (tx.type) {
                case TX_COMMIT:
                    // 若是commit 则放入commitXid并将对应的xid移出abort
                    Integer id = tx.xid;
                    abortXids.remove(id);
                    commitXids.add(tx.xid);
                    break;
                case TX_ABORT:
                    // 因为begin就放入abort，所以abort不用管
                    break;
                case TX_BEGIN:
                    abortXids.add(tx.xid);
                    break;
                default:
            }
        }

        Map<Character, List<Integer>> maps = new HashMap<>();
        maps.put('C', commitXids);
        maps.put('A', abortXids);
        return maps;
    }

    /**
     * 解析事务状态
     */
    private Tx parseTx(byte[] log) throws IOException {
        var tx = JBBPParser.prepare("byte type;int xid;").parse(log).mapTo(new Tx());
        return tx;
    }

    /**
     * 解析类型
     */
    private byte parseType(byte[] log) throws IOException {
        var type = JBBPParser.prepare("byte type;").parse(log).mapTo(new Type()).type;
        return type;
    }

    /**
     * 解析插入
     */
    private InsertLog parseInsert(byte[] log) throws IOException {
        var insertLog = JBBPParser.prepare("byte type;int xid;long uuid;int length;byte[length] item;")
                .parse(log).mapTo(new InsertLog());
        return insertLog;
    }

    /**
     * 解析更新
     */
    private UpdateLog parseUpdate(byte[] log) throws IOException {
        var updateLog = JBBPParser.prepare("byte type;int xid;long uuid;int length1;" +
                "byte[length1] itemBefore;int length2;byte[length2] itemAfter;").parse(log).mapTo(new UpdateLog());
        return updateLog;
    }

    private UpdateMetaLog parseUpdateMeta(byte[] log) throws IOException {
        var updateMeta = JBBPParser.prepare("byte type;int xid;long beforeUuid;" +
                "int length;byte[length] metadata;").
                parse(log).mapTo(new UpdateMetaLog());
        return updateMeta;
    }

    private boolean checkCommit(int xid, Map<Character, List<Integer>> maps) throws LogException {
        if (maps.get('C').contains(xid)) {
            return true;
        } else if (maps.get('A').contains(xid)) {
            return false;
        } else {
            throw new LogException(2);
        }
    }

    private void parseLog(byte[] log, IItemStorage itemStorage, Map<Character, List<Integer>> xidMaps) throws LogException {
        try {
            var type = parseType(log);
            switch (type) {
                case INSERT_FLAG:
                    System.out.println("正在解析插入");
                    var insertLog = parseInsert(log);
                    if (checkCommit(insertLog.xid, xidMaps)) {
                        // 如果事务已经提交，则redo
                        itemStorage.insertItemWithUuid(insertLog.item, insertLog.uuid);
                    } else {
                        // 如果事务没有提交，则undo
                        itemStorage.deleteUuid(insertLog.uuid);
                    }
                    break;
                case UPDATE_FLAG:
                    System.out.println("正在解析更新");
                    var updateLog = parseUpdate(log);
                    if (checkCommit(updateLog.xid, xidMaps)) {
                        // 若事务已经提交则redo
                        try {
                            itemStorage.updateItemWithoutLog(updateLog.uuid, updateLog.itemAfter);
                        } catch (UUIDException ignored) {
                            // uuid不存在说明对应之前的事务没有执行，是正常
                        }
                    } else {
                        // 若事务没有提交，则undo,恢复之前的数据
                        try {
                            itemStorage.updateItemWithoutLog(updateLog.uuid, updateLog.itemBefore);
                        } catch (UUIDException ignored) {

                        }
                    }
                    break;
                case METADATA_UPDATE_FLAG:
                    System.out.println("正在解析头信息更新");
                    var updateMetaLog = parseUpdateMeta(log);
                    if (checkCommit(updateMetaLog.xid, xidMaps)) {
                        // redo
                        itemStorage.setMetadataWithoutLog(updateMetaLog.metadata);
                    } else {
                        // undo
                        itemStorage.setMetaUuid(updateMetaLog.beforeUuid);
                    }
                    break;
                default:
                    return;
            }
        } catch (IOException | PageException e) {
            throw new LogException(1);
        }
    }

    /**
     * 日志头信息
     * TODO: 记录点等特殊要求
     */
    public static class LogHeader {
        @Bin
        int header;
    }

    /**
     * 解析类型
     */
    public static class Type {
        @Bin
        byte type;
    }

    /**
     * 事务状态的解析
     */
    @Data
    public static class Tx {
        @Bin
        byte type;
        @Bin
        int xid;
    }

    /**
     * 插入的解析
     */
    public static class InsertLog {
        @Bin
        byte type;
        @Bin
        int xid;
        @Bin
        long uuid;
        @Bin
        int length;
        @Bin
        byte[] item;

        public Object newInstance(Class<?> klazz) {
            return klazz == InsertLog.class ? new InsertLog() : null;
        }
    }

    /**
     * 更新的解析
     */
    public static class UpdateLog {
        @Bin
        byte type;
        @Bin
        int xid;
        @Bin
        long uuid;
        @Bin
        int length1;
        @Bin
        byte[] itemBefore;
        @Bin
        int length2;
        @Bin
        byte[] itemAfter;

        public Object newInstance(Class<?> klazz) {
            return klazz == UpdateLog.class ? new UpdateLog() : null;
        }
    }

    public static class UpdateMetaLog {
        @Bin
        byte type;
        @Bin
        int xid;
        @Bin
        long beforeUuid;
        @Bin
        int length;
        @Bin
        byte[] metadata;

        public Object newInstance(Class<?> klazz) {
            return klazz == UpdateMetaLog.class ? new UpdateMetaLog() : null;
        }
    }

    /**
     * 日志标志
     */
    final private static int HEADER = 4567;

    final private static byte INSERT_FLAG = 'i';

    final private static byte TX_BEGIN = 'b';

    final private static byte TX_ABORT = 'a';

    final private static byte TX_COMMIT = 'c';

    final private static byte UPDATE_FLAG = 'u';

    final private static byte METADATA_UPDATE_FLAG = 'm';


}
