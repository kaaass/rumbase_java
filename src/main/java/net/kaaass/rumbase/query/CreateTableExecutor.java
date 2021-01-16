package net.kaaass.rumbase.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.parse.stmt.CreateTableStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.table.Table;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.*;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.ArrayList;
import java.util.Locale;

/**
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class CreateTableExecutor implements Executable {

    @NonNull
    private final CreateTableStatement statement;

    @NonNull
    private final TableManager manager;

    @NonNull
    private final TransactionContext context;

    @Override
    public void execute() throws TableExistenceException, TableConflictException, ArgumentException {
        var tableName = statement.getTableName();
        var baseFields = new ArrayList<BaseField>();
        var dummyTable = new Table(tableName, baseFields);
        // FIXME: 2021/1/15 fix nullable
        var nullable = false;
        for (var def : statement.getColumnDefinitions()) {

            var fieldName = def.getColumnName();
            var fieldType = FieldType.valueOf(def.getColumnType().getTypeName().toUpperCase(Locale.ROOT));

            try {
                switch (fieldType) {
                    case INT:
                        baseFields.add(new IntField(fieldName, nullable, dummyTable));
                        break;
                    case FLOAT:
                        baseFields.add(new FloatField(fieldName, nullable, dummyTable));
                        break;
                    case VARCHAR:
                        baseFields.add(new VarcharField(fieldName, Integer.parseInt(def.getColumnType().getArguments().get(0)), nullable, dummyTable));
                        break;
                    default:
                        throw new TableConflictException(1);
                }
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                throw new ArgumentException(1);
            }

        }

        manager.createTable(context, tableName, baseFields, tableName + ".db");
    }
}
