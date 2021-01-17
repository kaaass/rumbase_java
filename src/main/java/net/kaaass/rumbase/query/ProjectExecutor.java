package net.kaaass.rumbase.query;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class ProjectExecutor implements Executable {

    @NonNull
    private final List<ColumnIdentifier> idrs;

    @NonNull
    private final List<ColumnIdentifier> selectedColumns;

    @NonNull
    private final List<List<Object>> data;

    @NonNull
    @Getter
    private final List<List<Object>> projectedResult = new ArrayList<>();

    @Override
    public void execute() throws TableExistenceException, IndexAlreadyExistException, TableConflictException, ArgumentException {
        var newColumnsLen = selectedColumns.size();
        var indexList = new ArrayList<Integer>();
        for (int i = 0; i < newColumnsLen; i++) {
            var col = selectedColumns.get(i);
            int finalI = i;
            idrs.forEach(idr -> {
                if (idr.getTableName().equals(col.getTableName()) && idr.getFieldName().equals(col.getFieldName())) {
                    indexList.add(finalI);
                }
            });
        }

        data.forEach(row -> {
            var newRow = new ArrayList<>();
            indexList.forEach(index -> newRow.add(row.get(index)));
            projectedResult.add(newRow);
        });
    }
}
