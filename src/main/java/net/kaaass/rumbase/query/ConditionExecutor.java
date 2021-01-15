package net.kaaass.rumbase.query;

import lombok.*;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.ConditionExpression;

import java.util.*;

/**
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class ConditionExecutor implements Executable {

    @NonNull
    private final List<ColumnIdentifier> idrs;

    @NonNull
    private final List<List<Object>> tableData;

    @NonNull
    private final ConditionExpression expression;

    @Getter
    private final List<List<Object>> result = new ArrayList<>();

    @Override
    public void execute() {

        var params = expression.getParams();
        var len = params.size();
        var paramMap = new HashMap<ColumnIdentifier, Object>(len);

        for (var row: tableData) {
            paramMap.clear();
            for (var param: params) {
                for (int i = 0; i < len; i++) {
                    var idr = idrs.get(i);
                    if (idr.getTableName().equals(param.getTableName()) && idr.getFieldName().equals(param.getFieldName())) {
                        paramMap.put(idr, row.get(i));
                    }
                }
            }
            if (expression.evaluate(paramMap)) {
                result.add(row);
            }
        }

    }
}
