package net.kaaass.rumbase.table.mock;

import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.table.*;
import net.kaaass.rumbase.table.exception.TableNotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock表结构，仅用于测试
 *
 * @author @KveinAxel
 */
@Deprecated
public class MockTable implements ITable {
    String tableName;
    List<IField> fields;

    public MockTable(String tableName, List<IField> fields) {
        this.tableName = tableName;
        this.fields = fields;
    }

    @Override
    public void parseSelf(byte[] raw) {

    }

    @Override
    public byte[] persistSelf() {
        return new byte[0];
    }

    @Override
    public int delete(TransactionContext context, String fieldName, UUID uuid, IRecordStorage recordStorage) {
        recordStorage.delete(context, uuid);
        // todo delete index
        return 0;
    }

    @Override
    public int update(TransactionContext context, String fieldName, UUID uuid, Entry newEntry) {
        // todo update
        return 0;
    }

    @Override
    public List<Entry> read(TransactionContext context, String fieldName, List<UUID> uuids, IRecordStorage recordStorage) throws TableNotFoundException {
        fields.stream()
                .filter(f -> f.getFieldName().equals(fieldName))
                .findAny()
                .orElseThrow(() -> new TableNotFoundException(2));

        var list = new ArrayList<Entry>();
        uuids.forEach(
                uuid -> recordStorage
                        .queryOptional(context, uuid)
                        .flatMap(this::parseEntry)
                        .ifPresent(list::add)
        );
        return list;
    }

    @Override
    public void insert(TransactionContext context, String fieldName, Entry newEntry, IRecordStorage recordStorage) throws TableNotFoundException, TypeIncompatibleException {
        fields.stream()
                .filter(f -> f.getFieldName().equals(fieldName))
                .findAny()
                .orElseThrow(() -> new TableNotFoundException(2));

        if (!newEntry.containsKey(fieldName)) {
            throw new TypeIncompatibleException(2);
        }

        entryToRaw(newEntry).ifPresent((entry) -> recordStorage.insert(context, entry));

        // todo add to index
    }

    @Override
    public List<UUID> search(String fieldName, FieldValue left, FieldValue right) throws TableNotFoundException {
        return fields
                .stream()
                .filter(f -> f.getFieldName().equals(fieldName))
                .findAny()
                .orElseThrow(() -> new TableNotFoundException(2))
                .searchRange(left, right);
    }

    @Override
    public Optional<Entry> strToEntry(List<String> values) {
        var entry = new Entry();
        if (values.size() != fields.size()) {
            return Optional.empty();
        }

        var iter = values.iterator();
        fields.forEach(f -> f.strToValue(iter.next()).ifPresent(value -> entry.put(f.getFieldName(), value)));

        return Optional.of(entry);
    }

    @Override
    public Optional<byte[]> entryToRaw(Entry entry) {

        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)
        ) {
            for (var e : entry.entrySet()) {
                var fieldValue = e.getValue();
                if (fieldValue.getValue() == FieldType.INT) {
                    dos.writeInt((int) fieldValue.getValue());
                } else if (fieldValue.getValue() == FieldType.FLOAT) {
                    dos.writeFloat((float) fieldValue.getValue());
                }

            }
            return Optional.of(baos.toByteArray());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Entry> parseEntry(byte[] raw) {
        AtomicInteger offset = new AtomicInteger();
        var entry = new Entry();

        var tot = fields.stream().map(IField::getSize).reduce(0, Integer::sum);
        if (tot != raw.length) {
            return Optional.empty();
        }

        fields.forEach(f -> {
            f.rawToValue(Arrays.copyOfRange(raw, offset.get(), offset.get() + f.getSize()))
                    .ifPresent(value -> entry.put(f.getFieldName(), value));
            offset.addAndGet(f.getSize());
        });

        return entry.isEmpty() ? Optional.empty() : Optional.of(entry);
    }
}
