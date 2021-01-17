package net.kaaass.rumbase.record.mock;


import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Mock记录存储，仅用于测试
 *
 * @author kaaass
 */
@Deprecated
@RequiredArgsConstructor
public class MockRecordStorage implements IRecordStorage {

    /**
     * 内存中创建的Mock存储
     */
    private static final Map<String, MockRecordStorage> MOCK_STORAGES = new HashMap<>();


    @Getter
    private final String mockId;

    private final Map<Long, byte[]> memoryStorage = new HashMap<>();

    private byte[] metadata = new byte[0];

    @Override
    public long insert(TransactionContext txContext, byte[] rawData) {
        var uuid = UUID.randomUUID().getLeastSignificantBits();
        this.memoryStorage.put(uuid, rawData);
        return uuid;
    }

    @Override
    public byte[] query(TransactionContext txContext, long recordId) throws RecordNotFoundException {
        if (!this.memoryStorage.containsKey(recordId)) {
            throw new RecordNotFoundException(1);
        }
        return this.memoryStorage.get(recordId);
    }

    @Override
    public Optional<byte[]> queryOptional(TransactionContext txContext, long recordId) {
        try {
            return Optional.ofNullable(query(txContext, recordId));
        } catch (RecordNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public void delete(TransactionContext txContext, long recordId) {
        this.memoryStorage.remove(recordId);
    }

    @Override
    public byte[] getMetadata(TransactionContext txContext) {
        return this.metadata;
    }

    @Override
    public void setMetadata(TransactionContext txContext, byte[] metadata) {
        this.metadata = metadata;
    }

    @Override
    public String getIdentifiedName() {
        return this.toString();
    }

    public static MockRecordStorage ofFile(String filepath) {
        var mockId = "file" + filepath;
        if (MOCK_STORAGES.containsKey(mockId)) {
            return MOCK_STORAGES.get(mockId);
        }
        var result = new MockRecordStorage(mockId);
        MOCK_STORAGES.put(mockId, result);
        return result;
    }

}
