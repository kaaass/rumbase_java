package net.kaaass.rumbase.record.mock;

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
public class MockRecordStorage implements IRecordStorage {

    /**
     * 内存中创建的Mock存储
     */
    private static final Map<String, MockRecordStorage> MOCK_STORAGES = new HashMap<>();

    private final String mockId;

    private final Map<UUID, byte[]> memoryStorage = new HashMap<>();

    private byte[] metadata = new byte[0];

    private MockRecordStorage(String mockId) {
        this.mockId = mockId;
    }

    @Override
    public UUID insert(TransactionContext txContext, byte[] rawData) {
        var uuid = UUID.randomUUID();
        this.memoryStorage.put(uuid, rawData);
        return uuid;
    }

    @Override
    public byte[] query(TransactionContext txContext, UUID recordId) throws RecordNotFoundException {
        if (!this.memoryStorage.containsKey(recordId)) {
            throw new RecordNotFoundException(1);
        }
        return this.memoryStorage.get(recordId);
    }

    @Override
    public void delete(TransactionContext txContext, UUID recordId) {
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

    public static MockRecordStorage ofFile(String filepath) {
        var mockId = "file" + filepath;
        if (MOCK_STORAGES.containsKey(mockId)) {
            return MOCK_STORAGES.get(mockId);
        }
        var result = new MockRecordStorage(mockId);
        MOCK_STORAGES.put(mockId, result);
        return result;
    }

    public String getMockId() {
        return mockId;
    }
}
