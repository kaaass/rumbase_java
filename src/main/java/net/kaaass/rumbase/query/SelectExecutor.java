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
        var joinExe = new InnerJoinExecutor(statement.getFromTable(), statement.getJoins(), manager, context);
        joinExe.execute();

        // 选择
        var conditionExe = new ConditionExecutor(joinExe.resultIdr, joinExe.resultRows, statement.getWhere());
        conditionExe.execute();

        var idrs = joinExe.resultIdr;
        // 排序
        var sortExe = new SortExecutor(idrs, conditionExe.getResult(), statement.getOrderBys(), manager);
        sortExe.execute();

        // 投影
        var projectExe = new ProjectExecutor(idrs, statement.getSelectColumns(), sortExe.getData());

        // 去重
        // FIXME: 2021/1/16 存储过大
        if (statement.isDistinct()) {
            var len = projectExe.getProjectedResult().size();
            var hashSet = new HashSet<List<Object>>(len);
            var resultList = new ArrayList<List<Object>>(len);
            projectExe.getProjectedResult().stream().filter(hashSet::add).map(resultList::add);
            resultData = resultList;
        } else {
            resultData = projectExe.getProjectedResult();
        }

        resultTable = statement.getSelectColumns();

    }
}
