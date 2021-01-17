package net.kaaass.rumbase.query;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.ConditionExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
        boolean ok;
        for (var row : tableData) {
            paramMap.clear();
            ok = true;
            for (var param : params) {
                for (int i = 0; i < idrs.size(); i++) {
                    var idr = idrs.get(i);
                    if (idr.getTableName().equals(param.getTableName()) && idr.getFieldName().equals(param.getFieldName())) {
                        var val = row.get(i);
                        if (val == null) {
                            ok = false;
                        } else {
                            paramMap.put(idr, val);
                        }
                        break;
                    }
                }
                if (!ok) {
                    break;
                }
            }
            if (ok && expression.evaluate(paramMap)) {
                result.add(row);
            }
        }

    }
}
