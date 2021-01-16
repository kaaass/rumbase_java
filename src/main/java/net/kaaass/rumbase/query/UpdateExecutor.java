package net.kaaass.rumbase.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.stmt.UpdateStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class UpdateExecutor implements Executable{

    @NonNull
    private final UpdateStatement statement;

    @NonNull
    private final TableManager manager;

    @NonNull
    private final TransactionContext context;

    @Override
    public void execute() throws TableExistenceException, ArgumentException, IndexAlreadyExistException, TableConflictException, RecordNotFoundException {
        var table = manager.getTable(statement.getTableName());
        var idrs = new ArrayList<ColumnIdentifier>();
        table.getFields().forEach(f -> idrs.add(new ColumnIdentifier(table.getTableName(), f.getName())));
        idrs.add(new ColumnIdentifier("__reserved__", "id"));

        var indexedField = table.getFirstIndexedField();
        if (indexedField == null) {
            throw new ArgumentException(2);
        }

        List<List<Object>> rows = new ArrayList<>();
        var iter = table.searchAll(indexedField.getName());
        while(iter.hasNext()) {
            var uuid = iter.next().getUuid();
            List<List<Object>> finalRows = rows;
            table.read(context, uuid).ifPresent(row -> {
                row.add(uuid);
                finalRows.add(row);
            });
        }

        var where = statement.getWhere();
        if (where != null) {
            var conditionExe = new ConditionExecutor(idrs, rows, where);
            conditionExe.execute();
            rows = conditionExe.getResult();
        }

        var uuids = new ArrayList<Long>();

        for (var row : rows) {
            uuids.add((long) row.remove(row.size() - 1));
        }

        var fields = table.getFields();
        var len = fields.size();
        var rowLen = rows.size();
        var updateFieldLen = statement.getColumns().size();

        for (int i = 0; i < len; i++) {
            for (int j = 0; j < updateFieldLen; j++) {
                var field = fields.get(i);
                if (field.getName().equals(statement.getColumns().get(j).getFieldName())) {
                    for (var list : rows) {
                        var val = fields.get(i).strToValue(statement.getValues().get(j));
                        list.set(i, val);
                    }
                }
            }
        }

        for (int i = 0; i < rowLen; i++) {
            table.updateObjs(context, uuids.get(i), rows.get(i));
        }

    }
}
