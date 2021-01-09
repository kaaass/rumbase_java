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
        l.add(3);
        l.add(2);
        iRecoveryStorage.begin(1,l);
    }
}
