package net.kaaass.rumbase.recovery;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.IItemStorage;
import net.kaaass.rumbase.dataitem.ItemManager;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * 对日志进行保存和恢复
 */
@Slf4j
public class IRecoveryTest extends TestCase {

    public void testBegin() throws PageException, LogException, FileException, IOException, UUIDException {
        String fileName = "testInsert.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = TransactionContext.empty();
        long uuid = iItemStorage.insertItem(txContext, bytes);

        assertArrayEquals(bytes, iItemStorage.queryItemByUuid(uuid));

        var recoveryStorage = iItemStorage.getRecoveryStorage();
        var logs = recoveryStorage.getContent();
        recoveryStorage.recovery();
    }
}
