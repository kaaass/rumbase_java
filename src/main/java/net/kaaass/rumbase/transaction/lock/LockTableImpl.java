package net.kaaass.rumbase.transaction.lock;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.transaction.exception.DeadlockException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 锁表的实现
 *
 * @author criki
 */
@Slf4j
public class LockTableImpl implements LockTable {

    /**
     * 单例
     */
    private static LockTable instance;
    /**
     * 锁表
     */
    private final Map<DataItemId, TxList> lockTable = new HashMap<>();
    /**
     * 锁表互斥锁
     */
    private final Lock lock = new ReentrantLock();

    private LockTableImpl() {

    }

    /**
     * 获取实例方法
     *
     * @return 单例
     */
    public static LockTable getInstance() {
        if (instance == null) {
            instance = new LockTableImpl();
        }
        return instance;
    }

    /**
     * 添加锁的模板函数
     *
     * @param xid  事务id
     * @param id   数据项id
     * @param mode 锁类型
     */
    private void addLockTemplate(int xid, DataItemId id, LockMode mode) throws DeadlockException {
        TxList list;

        // 获取id的等待队列
        lock.lock();
        try {
            if (lockTable.containsKey(id)) {
                list = lockTable.get(id);
            } else {
                list = new TxList();
                lockTable.put(id, list);
            }
        } finally {
            lock.unlock();
        }

        // 判断是否需要手动释放互斥锁
        boolean canUnlock = true;
        list.mutexLock.lock();
        try {
            // 判断是否能加锁
            boolean canGrant = list.canGrant(mode);
            log.info("{} can grant {} lock : {}", xid, mode, canGrant);
            // 虚加锁
            list.weakInsert(xid, id, mode, canGrant);
            // 检测死锁
            if (deadlockCheck()) {
                log.info("deadlock");
                list.pop();
                throw new DeadlockException(1);
            }

            // 可以加锁
            // 对于互斥锁，如果发生等待，在等待处即已释放，此处无需释放
            canUnlock = canGrant;

            // 移除虚锁
            list.pop();
            // 正式加锁
            list.insert(xid, id, mode, canGrant);
        } finally {
            if (canUnlock) {
                list.mutexLock.unlock();
            }
        }

    }

    /**
     * 添加共享锁
     *
     * @param xid       事务id
     * @param uuid      记录id
     * @param tableName 表名
     */
    @Override
    public void addSharedLock(int xid, long uuid, String tableName) throws DeadlockException {
        log.info("{} add shared lock on ({}, {})", xid, tableName, uuid);
        addLockTemplate(xid, new DataItemId(tableName, uuid), LockMode.SHARED);
    }

    /**
     * 添加排他锁
     *
     * @param xid       事务id
     * @param uuid      记录id
     * @param tableName 表名
     */
    @Override
    public void addExclusiveLock(int xid, long uuid, String tableName) throws DeadlockException {
        log.info("{} add exclusive lock on ({}, {})", xid, tableName, uuid);
        addLockTemplate(xid, new DataItemId(tableName, uuid), LockMode.EXCLUSIVE);
    }

    /**
     * 释放事务的锁
     *
     * @param xid 事务id
     */
    @Override
    public void release(int xid) {
        lock.lock();
        try {
            Set<DataItemId> dataItemSet = new HashSet<>();
            List<DataItemId> sharedLocks = TxList.sharedLocks.get(xid);
            List<DataItemId> exclusiveLocks = TxList.exclusiveLocks.get(xid);
            log.info("{}'s sharedLocks: {}", xid, sharedLocks);
            log.info("{}'s exclusiveLocks: {}", xid, exclusiveLocks);
            if (sharedLocks != null) {
                dataItemSet.addAll(sharedLocks);
            }
            if (exclusiveLocks != null) {

                dataItemSet.addAll(exclusiveLocks);
            }

            for (DataItemId id : dataItemSet) {
                release(xid, id);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放事务xid持有的数据项id的锁
     *
     * @param xid 事务id
     * @param id  数据项id
     */
    private void release(int xid, DataItemId id) {
        TxList waitList;

        // 获取id的等待队列
        lock.lock();
        try {
            waitList = lockTable.get(id);
        } finally {
            lock.unlock();
        }

        // 从等待队列中移除xid
        waitList.mutexLock.lock();
        try {
            // 查找等待队列中事务id为xid的项
            List<TxItem> lockList = waitList.locks.stream().filter(lock -> lock.xid == xid).collect(Collectors.toList());

            List<DataItemId> txSharedLocks = TxList.sharedLocks.get(xid);
            List<DataItemId> txExclusiveLocks = TxList.exclusiveLocks.get(xid);
            for (TxItem lock : lockList) {
                // 从事务持锁列表中移除该数据项
                if (lock.mode.equals(LockMode.SHARED)) {
                    txSharedLocks.remove(id);
                } else {
                    txExclusiveLocks.remove(id);
                }

                // 中止锁申请
                lock.abort();

                // 从数据项等待队列中移除该事务锁
                waitList.locks.remove(lock);
            }

            // 数据项等待队列空，从锁表中移除
            if (waitList.locks.isEmpty()) {
                lockTable.remove(id);
                return;
            }

        } finally {
            waitList.mutexLock.unlock();
        }

        boolean firstTx = true;
        // 等待队列非空，从等待队列中唤醒下一个事务
        for (TxItem tx : waitList.locks) {
            if (tx.granted) {
                // 有事务持有该数据项的锁，暂不分配新锁
                break;
            }

            // 给第一个事务加锁
            if (firstTx) {
                tx.grant();
                firstTx = false;
            }
            // 给非第一个事务且申请共享锁的事务加锁
            else if (tx.mode.equals(LockMode.SHARED)) {
                tx.grant();
                continue;
            }

            // 如果该事务是排他锁，则不给之后的事务加锁
            if (tx.mode.equals(LockMode.EXCLUSIVE)) {
                break;
            }
        }
    }

    /**
     * 检查是否存在死锁
     *
     * @return <table>
     * <tr><td>true</td> <td>存在死锁</td></tr>
     * <tr><td>false</td> <td>不存在死锁</td></tr>
     * </table>
     */
    private boolean deadlockCheck() {
        Graph graph = new Graph();

        // 建图

        // 遍历每一个等待队列
        var lockTableView = Collections.unmodifiableMap(lockTable);
        log.debug("Lock table: {}", lockTableView);
        for (TxList list : lockTableView.values()) {
            List<TxItem> waitingTxs = new ArrayList<>(list.locks);
            log.debug("locks: {}", list.locks);
            // 对等待队列中建立等待关系
            for (int i = 0; i < waitingTxs.size() - 1; i++) {
                TxItem frontItem = waitingTxs.get(i);
                // 共享锁
                if (frontItem.mode.equals(LockMode.SHARED)) {
                    // 找到后面第一个排他锁
                    for (int j = i + 1; j < waitingTxs.size(); j++) {
                        TxItem backItem = waitingTxs.get(j);
                        // 邻近的共享锁不构成等待关系
                        if (backItem.mode.equals(LockMode.SHARED)) {
                            continue;
                        }

                        log.debug("[CREATING GRAPH] add edge : {} -> {}", backItem.xid, frontItem.xid);

                        // backItem等待frontItem
                        graph.addEdge(backItem.xid, frontItem.xid);
                    }
                }
                // 排他锁
                else {
                    // 邻近的后面的锁等待该锁
                    TxItem backItem = waitingTxs.get(i + 1);
                    // backItem等待frontItem
                    log.debug("[CREATING GRAPH] add edge : {} -> {}", backItem.xid, frontItem.xid);
                    graph.addEdge(backItem.xid, frontItem.xid);
                }
            }
        }

        log.debug("create graph successful: {}", graph);
        // 图成环，则有死锁
        return graph.hasLoop();
    }
}
