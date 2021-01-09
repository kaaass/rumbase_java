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

    @Override
    public String toString() {
        return "Pair{" +
                "key=" + key +
                ", uuid=" + uuid +
                '}';
    }

    public long getUuid() {
        return uuid;
    }
}
