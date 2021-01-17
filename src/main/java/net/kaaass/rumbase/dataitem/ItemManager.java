package net.kaaass.rumbase.dataitem;


import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据项管理类
 *
 * @author kaito
 */

public class ItemManager {

    static Map<String, IItemStorage> maps = new HashMap<>();

    /**
     * 通过文件名解析得到数据项管理器
     *
     * @param fileName 文件名
     * @return 数据项管理器，用于管理数据项
     */
    public static IItemStorage fromFile(String fileName) throws FileException, IOException, PageException {
        if (maps.containsKey(fileName)) {
            return maps.get(fileName);
        } else {
            IItemStorage iItemStorage = ItemStorage.ofFile(fileName);
            maps.put(fileName, iItemStorage);
            return iItemStorage;
        }
    }

    /**
     * 新建一个数据库，并且将上层提供的头信息写入。
     *
     * @param fileName  文件名
     * @param metadata  上层提供的表头信息
     * @param txContext 对应的事务名
     * @return 数据项管理器
     * @throws FileException 想新建的文件已经存在的异常
     */
    public static IItemStorage createFile(TransactionContext txContext, String fileName, byte[] metadata) throws FileException, IOException, PageException {
        // 如果文件已经存在，那么就抛出文件已存在异常
        if (maps.containsKey(fileName)) {
            throw new FileException(1);
        } else {
            // 若文件不存在，则创建文件。
            IItemStorage iItemStorage = ItemStorage.ofNewFile(txContext, fileName, metadata);
            maps.put(fileName, iItemStorage);
            return iItemStorage;
        }

    }
}
