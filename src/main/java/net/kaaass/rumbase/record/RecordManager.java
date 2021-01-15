package net.kaaass.rumbase.record;

import lombok.SneakyThrows;
import net.kaaass.rumbase.dataitem.ItemManager;

/**
 * 记录管理类
 *
 * @author kaaass
 */
public class RecordManager {


    /**
     * 从文件获取记录存储对象
     *
     * @param filepath 记录文件
     * @return 记录存储对象
     */
    @SneakyThrows
    public static IRecordStorage fromFile(String filepath) {
        var itemStorage = ItemManager.fromFile(filepath);
        var identifier = "TBL_" + filepath;
        return new MvccRecordStorage(itemStorage, identifier);
    }
}
