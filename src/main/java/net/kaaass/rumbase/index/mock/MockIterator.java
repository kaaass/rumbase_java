package net.kaaass.rumbase.index.mock;

import net.kaaass.rumbase.index.Pair;

import java.util.Iterator;
import java.util.LinkedList;

public class MockIterator implements Iterator {
    private MockBtreeIndex mockBtreeIndex;
    private Iterator tempIterator;
    private long tempKey;
    private boolean state = true;

    public MockIterator(MockBtreeIndex mockBtreeIndex) {
        this.mockBtreeIndex = mockBtreeIndex;
        var v = mockBtreeIndex.getHashMap().entrySet().iterator().next();
        tempIterator = v.getValue().iterator();
        tempKey = v.getKey();
    }

    public MockIterator(MockBtreeIndex mockBtreeIndex,long keyHash,boolean isWith) {
        this.mockBtreeIndex = mockBtreeIndex;
        if (isWith == true){
            do {
                var v = mockBtreeIndex.getHashMap().entrySet().iterator().next();
                if (v == null) {
                    state = false;
                    break;
                }
                tempIterator = v.getValue().iterator();
                tempKey = v.getKey();
            } while (tempKey < keyHash);
        }
        else
        {
            do {
                var v = mockBtreeIndex.getHashMap().entrySet().iterator().next();
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
        return mockBtreeIndex.getHashMap().entrySet().iterator().hasNext();
    }


    @Override
    public Object next() {
        if (state == false) return null;
        Pair res = new Pair(tempKey, (Long) tempIterator.next());
        if (!tempIterator.hasNext()) {
            var v = mockBtreeIndex.getHashMap().entrySet().iterator().next();
            if (v == null) return null;
            tempIterator = v.getValue().iterator();
            tempKey = v.getKey();
        }
        return res;
    }
}
