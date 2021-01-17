package net.kaaass.rumbase.transaction.lock;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务项列表
 * <p>
 * 锁表中一个数据项的事务等待列表
 *
 * @author criki
 */
@Slf4j
public class TxList {

    /**
     * 记录事务持有的共享锁
     */
    public static Map<Integer, List<DataItemId>> sharedLocks = new HashMap<>();

    /**
     * 记录事务持有的排他锁
     */
    public static Map<Integer, List<DataItemId>> exclusiveLocks = new HashMap<>();

    /**
     * 一个数据项上的事务请求列表
     */
    public Deque<TxItem> locks = new ArrayDeque<>();

    /**
     * 事务列表的互斥锁
     */
    public Lock mutexLock = new ReentrantLock();

    /**
     * 判断当前等待列表是否支持分配mode类型的锁
     *
     * @param mode 待分配锁类型
     * @return 是否可以分配
     */
    public boolean canGrant(LockMode mode) {
        // 等待队列为空，可以分配任何锁
        if (locks.isEmpty()) {
            return true;
        }

        // 非空队列中，不可申请排他锁
        if (mode.equals(LockMode.EXCLUSIVE)) {
            return false;
        }

        // 当前锁类型为共享锁，且等待队列非空

        // 获取队尾事务项
        TxItem item = locks.getLast();

        // 当且仅当队尾事务项锁类型是共享锁
        // 且已得到锁时，才可申请共享锁
        return item.mode.equals(LockMode.SHARED) && item.granted;
    }

    /**
     * 向等待列表插入一个事务项
     *
     * @param xid     事务id
     * @param id      数据项id
     * @param mode    申请锁类型
     * @param granted 是否已分配锁
     */
    public void insert(int xid, DataItemId id, LockMode mode, boolean granted) {

        log.info("{} getting {} lock on {}, it's {}", xid, mode, id, granted);

        // 创建事务项
        TxItem item = new TxItem(granted, xid, mode);
        // 加入等待队列
        locks.add(item);

        // 未分配锁时，令其等待
        if (!granted) {
            // 先将互斥锁解开，放行后面对等待队列的操作
            mutexLock.unlock();
            // 等待
            item.waitForGrant();
        }


        // 记录事务获得到的数据项锁
        List<DataItemId> lockList;
        if (mode.equals(LockMode.SHARED)) {
            if (sharedLocks.containsKey(xid)) {
                lockList = sharedLocks.get(xid);
            } else {
                lockList = new ArrayList<>();
                sharedLocks.put(xid, lockList);
            }
        } else {
            if (exclusiveLocks.containsKey(xid)) {
                lockList = exclusiveLocks.get(xid);
            } else {
                lockList = new ArrayList<>();
                exclusiveLocks.put(xid, lockList);
            }
        }

        lockList.add(id);
    }

    /**
     * 弹出最后一个事务项
     */
    public void pop() {
        locks.pollLast();
    }

    /**
     * 向等待列表插入一个事务项，用于判断死锁
     *
     * @param xid     事务id
     * @param id      数据项id
     * @param mode    申请锁类型
     * @param granted 是否已分配锁
     */
    public void weakInsert(int xid, DataItemId id, LockMode mode, boolean granted) {
        // 创建事务项
        TxItem item = new TxItem(granted, xid, mode);
        // 加入等待队列
        locks.add(item);
    }
}
