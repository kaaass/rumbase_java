package net.kaaass.rumbase.recovery;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.record.IRecordStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class IRecoveryTest extends TestCase {


    public void testBegin(){
        IRecoveryStorage iRecoveryStorage = RecoveryManager.recovery("user.db");
        List<Integer> l = new ArrayList<>();
        int xid = 1;
        l.add(3);
        l.add(2);
        iRecoveryStorage.begin(xid,l);

        var content = iRecoveryStorage.getContent();
        String beginStr = "begin " + xid;
        String snapStr = "snap " + l.toString();
        List<byte[]> result = new ArrayList<>();
        result.add(beginStr.getBytes());
        result.add(snapStr.getBytes());
        assertEquals(result,content);
    }



}
