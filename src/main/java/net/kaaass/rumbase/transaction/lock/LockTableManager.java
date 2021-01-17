package net.kaaass.rumbase.transaction.lock;

import net.kaaass.rumbase.transaction.mock.MockLockTable;

import java.util.HashMap;
import java.util.Map;

/**
 * 锁表管理器
 * <p>
 * 管理锁表
 *
 * @author criki
 */
public class LockTableManager {
    /**
     * 表名与锁表的映射
     */
    private final Map<String, LockTable> lockTables;

    /**
     * 锁表管理器
     */
    public LockTableManager() {
        this.lockTables = new HashMap<>();
    }

    public LockTable getTable(String tableName) {
        if (lockTables.containsKey(tableName)) {
            return lockTables.get(tableName);
        } else {
            LockTable table = new MockLockTable();
            lockTables.put(tableName, table);
            return table;
        }
    }
}
