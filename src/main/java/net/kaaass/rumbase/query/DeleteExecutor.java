package net.kaaass.rumbase.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.stmt.DeleteStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class DeleteExecutor implements Executable {

    @NonNull
    private final DeleteStatement statement;

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

        var field = table.getFirstIndexedField();
        if (field == null) {
            throw new ArgumentException(2);
        }

        List<List<Object>> rows = new ArrayList<>();
        var iter = table.searchAll(field.getName());
        while (iter.hasNext()) {
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

        for (var row : rows) {
            try {
                table.delete(context, (long) row.get(row.size() - 1));
            } catch (ClassCastException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
