package net.kaaass.rumbase.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.parse.stmt.CreateTableStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
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
    public void execute() throws TableExistenceException, TableConflictException, ArgumentException, RecordNotFoundException {
        var tableName = statement.getTableName();
        var baseFields = new ArrayList<BaseField>();
        boolean nullable;
        for (var def : statement.getColumnDefinitions()) {
            nullable = !def.isNotNull();
            var fieldName = def.getColumnName();
            var fieldType = FieldType.valueOf(def.getColumnType().getTypeName().toUpperCase(Locale.ROOT));

            try {
                switch (fieldType) {
                    case INT:
                        baseFields.add(new IntField(fieldName, nullable, null));
                        break;
                    case FLOAT:
                        baseFields.add(new FloatField(fieldName, nullable, null));
                        break;
                    case VARCHAR:
                        baseFields.add(new VarcharField(fieldName, Integer.parseInt(def.getColumnType().getArguments().get(0)), nullable, null));
                        break;
                    default:
                        throw new TableConflictException(1);
                }
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                throw new ArgumentException(1);
            }

        }

        manager.createTable(context, tableName, baseFields, "data/table/" + tableName + ".db");
    }
}
