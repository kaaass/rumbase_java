package net.kaaass.rumbase.recovery;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * TODO 文档
 */
@Slf4j
public class IRecoveryTest extends TestCase {

    public void testBegin() {
        IRecoveryStorage iRecoveryStorage = RecoveryManager.recovery("user.db");
        List<Integer> l = new ArrayList<>();
        int xid = 1;
        l.add(3);
        l.add(2);
        iRecoveryStorage.begin(xid, l);

        var content = iRecoveryStorage.getContent();
        String beginStr = "begin " + xid;
        String snapStr = "snap " + l.toString();
        List<byte[]> result = new ArrayList<>();
        result.add(beginStr.getBytes());
        result.add(snapStr.getBytes());
        assertEquals(result.size(), content.size());
        assertArrayEquals(result.get(0), content.get(0));
        assertArrayEquals(result.get(1), content.get(1));
    }
}
