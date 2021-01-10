package net.kaaass.rumbase.index.mock;

import net.kaaass.rumbase.index.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MockIterator implements Iterator<Pair> {
    private Iterator<Map.Entry<Long, List<Long>>> indexIterator;
    private Iterator<Long> tempIterator;
    private long tempKey;
    private boolean state = true;

    public MockIterator(MockBtreeIndex mockBtreeIndex) {
        indexIterator = mockBtreeIndex.getHashMap().entrySet().iterator();
        if (!indexIterator.hasNext()) {
            state = false;
            return;
        }
        var f = indexIterator.next();
        tempIterator = f.getValue().iterator();
        tempKey = f.getKey();
    }

    public MockIterator(MockBtreeIndex mockBtreeIndex, long keyHash, boolean isWith) {
        indexIterator = mockBtreeIndex.getHashMap().entrySet().iterator();
        if (isWith == true) {
            do {
                var v = indexIterator.next();
                if (v == null) {
                    state = false;
                    break;
                }
                tempIterator = v.getValue().iterator();
                tempKey = v.getKey();
            } while (tempKey < keyHash);
        } else {
            do {
                var v = indexIterator.next();
                if (v == null) {
                    state = false;
                    break;
                }
                tempIterator = v.getValue().iterator();
                tempKey = v.getKey();
            } while (tempKey <= keyHash);
        }
    }

    @Override
    public boolean hasNext() {
        return indexIterator.hasNext() || (tempIterator != null && tempIterator.hasNext());
    }


    @Override
    public Pair next() {
        if (state == false) {
            return null;
        }
        Pair res = new Pair(tempKey, tempIterator.next());
        if (!tempIterator.hasNext()) {
            if (!indexIterator.hasNext()) {
                state = false;
                return res;
            }
            var v = indexIterator.next();
            tempIterator = v.getValue().iterator();
            tempKey = v.getKey();
        }
        return res;
    }
}
