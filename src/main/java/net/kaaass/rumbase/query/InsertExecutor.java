package net.kaaass.rumbase.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.stmt.InsertStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.ArrayList;

/**
 *
 *
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class InsertExecutor implements Executable{

    @NonNull
    private final InsertStatement statement;

    @NonNull
    private final TableManager manager;

    @NonNull
    private final TransactionContext context;

    @Override
    public void execute() throws TableExistenceException, TableConflictException, ArgumentException {
        var table = manager.getTable(statement.getTableName());

        var columns = statement.getColumns();
        if (columns == null || columns.isEmpty()) {
            if (columns == null) {
                columns = new ArrayList<>();
            }
            var finalColumns = columns;
            table.getFields().forEach(f -> finalColumns.add(new ColumnIdentifier(table.getTableName(), f.getName())));
        }
        var len = columns.size();
        var insertArray = new ArrayList<String>();
        boolean ok;

        for (BaseField f : table.getFields()) {
            ok = false;
            for (int j = 0; j < len; j++) {
                var insertField = columns.get(j);
                if (f.getName().equals(insertField.getFieldName())) {

                    insertArray.add(statement.getValues().get(j));
                    ok = true;

                }
            }
            if (!ok) {
                insertArray.add("");
            }
        }

        table.insert(context, insertArray);

    }
}
