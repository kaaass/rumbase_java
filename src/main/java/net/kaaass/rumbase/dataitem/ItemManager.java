package net.kaaass.rumbase.dataitem;

import net.kaaass.rumbase.dataitem.exception.FileExistException;
import net.kaaass.rumbase.dataitem.mock.MockItemStorage;

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
    public static IItemStorage fromFile(String fileName) throws FileExistException {

        if (fileName.equals("error.db")) {
            throw new FileExistException(2);
        }

        if (maps.containsKey(fileName)) {
            return maps.get(fileName);
        } else {
            IItemStorage iItemStorage = MockItemStorage.ofFile(fileName);
            maps.put(fileName, iItemStorage);
            return iItemStorage;
        }
    }

    /**
     * 新建一个数据库，并且将上层提供的头信息写入。
     *
     * @param fileName 文件名
     * @param metadata 上层提供的表头信息
     * @return 数据项管理器
     * @throws FileExistException 想新建的文件已经存在的异常
     */
    public static IItemStorage createFile(String fileName, byte[] metadata) throws FileExistException {
        // 如果文件已经存在，那么就抛出文件已存在异常
        if (maps.containsKey(fileName)) {
            throw new FileExistException(1);
        } else {
            // 若文件不存在，则创建文件。
            IItemStorage iItemStorage = MockItemStorage.ofNewFile(fileName, metadata);
            maps.put(fileName, iItemStorage);
            return iItemStorage;
        }

    }
}
