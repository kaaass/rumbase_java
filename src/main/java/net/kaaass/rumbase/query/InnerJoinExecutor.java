package net.kaaass.rumbase.query;

import lombok.NonNull;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.SelectStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 *
 * @author @KveinAxel
 */
public class InnerJoinExecutor extends AbstractJoinExecutor{


    public InnerJoinExecutor(@NonNull String fromTable, @NonNull List<SelectStatement.JoinTable> joins, TableManager manager, TransactionContext context) {
        super(fromTable, joins, manager, context);
    }

    @Override
    public void execute() throws TableExistenceException, TableConflictException, ArgumentException {
        var table = manager.getTable(fromTable);
        var rows = table.readAll(context);

        var idrs = new ArrayList<ColumnIdentifier>();
        table
                .getFields()
                .forEach(
                        f -> idrs.add(new ColumnIdentifier(table.getTableName(), f.getName()))
                );


        for (var join: joins) {
            var joinTable = manager.getTable(join.getTableName());
            var joinRows = joinTable.readAll(context);
            var intermediateRows = new ArrayList<List<Object>>();
            var joinIdrs = new ArrayList<ColumnIdentifier>();

            joinTable
                    .getFields()
                    .forEach(
                            f -> joinIdrs.add(new ColumnIdentifier(joinTable.getTableName(), f.getName()))
                    );

            var len = idrs.size();
            var joinLen = joinIdrs.size();
            var paramMap = new HashMap<ColumnIdentifier, Object>(idrs.size());
            var params = join.getJoinOn().getParams();

            for (var row: rows) {
                for (var joinRow: joinRows) {
                    paramMap.clear();
                    for (var param: params) {

                        for (int i = 0; i < len; i++) {
                            var idr = idrs.get(i);
                            if (idr.getTableName().equals(param.getTableName()) && idr.getFieldName().equals(param.getFieldName())) {
                                paramMap.put(idrs.get(i), row.get(i));
                            }
                        }
                        for (int i = 0; i < joinLen; i++) {
                            var idr = idrs.get(i);
                            if (idr.getTableName().equals(param.getTableName()) && idr.getFieldName().equals(param.getFieldName())) {
                                paramMap.put(joinIdrs.get(i), joinRow.get(i));
                            }
                        }
                    }
                    if (join.getJoinOn().evaluate(paramMap)) {
                        intermediateRows.add(
                                Stream
                                        .of(row, joinRow)
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toList())
                        );

                    }


                }
            }

            rows = intermediateRows;
            idrs.addAll(joinIdrs);
        }

        resultIdr = idrs;
        resultRows = rows;
    }
}
