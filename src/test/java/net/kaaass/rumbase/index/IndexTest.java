package net.kaaass.rumbase.index;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataItem.IItemStorage;
import net.kaaass.rumbase.dataItem.ItemManager;
import net.kaaass.rumbase.dataItem.exception.FileExistException;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.index.exception.IndexNotFoundException;

import java.util.Iterator;
import java.util.Random;

/**
 * 对索引部分进行测试
 *
 * @author DoctorWei1314
 * @see net.kaaass.rumbase.index.Index
 */
@Slf4j
public class IndexTest extends TestCase {
    /**
     * 测试索引对象管理与拿取
     */
    public void testIndexManagement() {
        //测试索引是否存在，表示student表的id字段索引，table_name$field_name
        assertFalse("don't exists such a index", Index.exists("student$id"));

        //创建一个空索引，如果已经存在，则抛出异常
        try {
            Index.createEmptyIndex("student$id");
            Index.createEmptyIndex("student$name");
            Index.createEmptyIndex("student$score");
            Index.createEmptyIndex("student$score");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        //拿到这个索引,若没有则抛出异常
        try {
            Index.getIndex("student$id");
            Index.getIndex("employee$id");
        } catch (IndexNotFoundException e) {
            log.error("Exception Error :", e);
        }
    }

    /**
     * 测试索引的插入与第一个迭代器功能
     */
    public void testInsert(){
        Index testIndex = null;
        try {
            testIndex = Index.createEmptyIndex("student$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        for (int i = 0; i < 50; i++) {
            testIndex.insert(i, new Random().nextLong());
            testIndex.insert(i,new Random().nextLong());
        }

        //拿到第一个迭代器，遍历去全索引
        Iterator<Pair> iterator = testIndex.getFirst();
        while (iterator.hasNext()){
            log.info(iterator.next().toString());
        }
    }

    /**
     * 测试索引的查询功能
     */
    public void testquery(){
        Index testIndex = null;
        try {
            testIndex = Index.createEmptyIndex("student$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        for (int i = 5; i > 0; i--) {
            testIndex.insert(i, new Random().nextLong());
            testIndex.insert(i,new Random().nextLong());
        }

        //keyHash在内的迭代器 eg: keyHash = 4,   33^^^445577
        Iterator<Pair> iterator1 = testIndex.queryWith(3);
        while (iterator1.hasNext()){
            log.info(iterator1.next().toString());
        }

        //不包括keyHash在内的迭代器 eg: keyHash = 4,   3344^^^5577
        Iterator<Pair> iterator2 = testIndex.queryWithout(3);
        while (iterator2.hasNext()){
            log.info(iterator2.next().toString());
        }

        //
        var f = testIndex.query(4);
        for (Long s : f){
            log.info("query4:"+s.toString());
        }
    }
}
