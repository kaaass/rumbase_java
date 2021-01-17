package net.kaaass.rumbase.query;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.stmt.SelectStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class SelectExecutor implements Executable{

    @NonNull
    private final SelectStatement statement;

    @NonNull
    private final TableManager manager;

    @NonNull
    private final TransactionContext context;

    @Getter
    private List<ColumnIdentifier> resultTable;

    @Getter
    private List<List<Object>> resultData;

    @Override
    public void execute() throws TableConflictException, ArgumentException, TableExistenceException, IndexAlreadyExistException, RecordNotFoundException {


        // 连接
        var joins = statement.getJoins();
        if (joins == null) {
            joins = new ArrayList<>();
        }
        var joinExe = new InnerJoinExecutor(statement.getFromTable(), joins, manager, context);
        joinExe.execute();
        resultData = joinExe.resultRows;
        resultTable = joinExe.resultIdr;

        // 选择
        var where = statement.getWhere();
        if (where != null) {
            var conditionExe = new ConditionExecutor(resultTable, resultData, where);
            conditionExe.execute();
            resultData = conditionExe.getResult();
        }

        // 排序
        var orderBys = statement.getOrderBys();
        if (orderBys != null) {
            var sortExe = new SortExecutor(resultTable, resultData, orderBys, manager);
            sortExe.execute();
            resultData = sortExe.getData();
        }

        // 投影
        var selectCols = statement.getSelectColumns();
        if (selectCols != null && !selectCols.isEmpty()) {
            var projectExe = new ProjectExecutor(resultTable, selectCols, resultData);
            projectExe.execute();
            resultData = projectExe.getProjectedResult();
            resultTable = selectCols;
        }

        // 去重
        // FIXME: 2021/1/16 存储过大
        if (statement.isDistinct()) {
            var len = resultData.size();
            var hashSet = new HashSet<List<Object>>(len);
            var resultList = new ArrayList<List<Object>>(len);

            for (var item: resultData) {
                if (hashSet.add(item)) {
                    resultList.add(item);
                }
            }

            resultData = resultList;
        }

    }
}
