package net.kaaass.rumbase.query;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.SelectStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;

import java.util.List;

/**
 *
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class SortExecutor implements Executable {

    @NonNull
    private final List<ColumnIdentifier> idrs;

    @Getter
    @NonNull
    private final List<List<Object>> data;

    @NonNull
    private final List<SelectStatement.OrderBy> orderBys;

    @NonNull
    private final TableManager manager;


    @Override
    public void execute() throws TableExistenceException, IndexAlreadyExistException, TableConflictException, ArgumentException {
        var len = idrs.size();
        for (var orderBy : orderBys) {

            int index = -1;
            ColumnIdentifier orderIdr = null;
            for (int i = 0; i < len; i++) {
                var column = orderBy.getColumn();
                var idr = idrs.get(i);
                if (idr.getTableName().equals(column.getTableName()) && idr.getFieldName().equals(column.getFieldName())) {
                    index = i;
                    orderIdr = idr;
                    break;
                }
            }

            if (index == -1) {
                throw new ArgumentException(1);
            }

            var table = manager.getTable(orderIdr.getTableName());
            var field = table
                    .getField(orderBy.getColumn().getFieldName())
                    .orElseThrow(() -> new ArgumentException(1));

            final int finalIndex = index;
            if (orderBy.isAscending()) {
                data.stream().sorted((list1, list2) -> {
                    try {
                        return field.compare(list1.get(finalIndex), list2.get(finalIndex));
                    } catch (TableConflictException e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                data.stream().sorted((list1, list2) -> {
                    try {
                        return field.compare(list2.get(finalIndex), list1.get(finalIndex));
                    } catch (TableConflictException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

        }
    }
}
