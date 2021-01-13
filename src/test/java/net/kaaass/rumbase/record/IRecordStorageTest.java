package net.kaaass.rumbase.record;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;

/**
 * 测试记录存储接口
 *
 * @see net.kaaass.rumbase.record.IRecordStorage
 * @author kaaass
 */
@Slf4j
public class IRecordStorageTest extends TestCase {

    public void testQuery() {
        var storage = RecordManager.fromFile("test_query");
        var context = TransactionContext.empty();

        var result = storage.queryOptional(context, UUID.randomUUID().getLeastSignificantBits());

        assertTrue("unknown uuid should get empty", result.isEmpty());

        try {
            storage.query(context, UUID.randomUUID().getLeastSignificantBits());
            fail("unknown uuid should get exception");
        } catch (RecordNotFoundException e) {
            log.error("Exception expected: ", e);
        }
    }

    public void testInsert() {
        var storage = RecordManager.fromFile("test_insert");
        var context = TransactionContext.empty();

        var id = storage.insert(context, new byte[]{0x1, 0x2, 0x1f});
        var result = storage.queryOptional(context, id);

        assertTrue("result should present", result.isPresent());
        assertArrayEquals(new byte[]{0x1, 0x2, 0x1f}, result.get());
    }

    public void testDelete() {
        var storage = RecordManager.fromFile("test_delete");
        var context = TransactionContext.empty();

        storage.insert(context, new byte[]{0x1, 0x2});
        storage.insert(context, new byte[]{0x7, 0x3, 0x1f});
        var id = storage.insert(context, new byte[]{0x54, 0x23, 0x23, 0x44});
        var result = storage.queryOptional(context, id);
        assertTrue("result should present", result.isPresent());

        storage.delete(context, id);
        result = storage.queryOptional(context, id);

        assertTrue("record should be deleted", result.isEmpty());
    }

    public void testMetadata() {
        var storage = RecordManager.fromFile("test_metadata");
        var context = TransactionContext.empty();

        var result = storage.getMetadata(context);
        assertArrayEquals("default metadata should be empty", new byte[0], result);

        storage.setMetadata(context, new byte[]{0x23, 0x45, 0x67});
        assertArrayEquals(new byte[]{0x23, 0x45, 0x67}, storage.getMetadata(context));
    }
}