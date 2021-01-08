package net.kaaass.rumbase.index;

public class Pair {
    private long key;
    private long uuid;

    public Pair(long key, long uuid) {
        this.key = key;
        this.uuid = uuid;
    }

    public long getKey() {
        return key;
    }

    public long getUuid() {
        return uuid;
    }
}
