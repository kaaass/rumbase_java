package net.kaaass.rumbase.transaction.mock;

import net.kaaass.rumbase.transaction.LockTable;

/**
 * Mock锁表
 *
 * @author criki
 */

@Deprecated
public class MockLockTable implements LockTable {


    @Override
    public void addSharedLock(int xid, long uuid, String tableName) {

    }

    @Override
    public void addExclusiveLock(int xid, long uuid, String tableName) {

    }

    @Override
    public void release(int xid) {

    }
}
