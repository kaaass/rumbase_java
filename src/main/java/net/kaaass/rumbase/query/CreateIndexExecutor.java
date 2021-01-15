package net.kaaass.rumbase.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.CreateIndexStatement;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.BaseField;

/**
 *
 *
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class CreateIndexExecutor implements Executable{

    @NonNull
    private final CreateIndexStatement statement;

    @NonNull
    private final TableManager manager;

    @Override
    public void execute() throws TableExistenceException, IndexAlreadyExistException {
        var table = manager.getTable(statement.getTableName());
        BaseField field = null;

        for (var tableField: table.getFields()) {
            for (var column: statement.getColumns()) {
                if (tableField.getName().equals(column.getFieldName())) {
                    field = tableField;
                    field.setIndexName(statement.getIndexName());
                    break;
                }
            }
        }

        if (field != null) {
            field.createIndex();
        } else {
            throw new TableExistenceException(2);
        }
    }
}
