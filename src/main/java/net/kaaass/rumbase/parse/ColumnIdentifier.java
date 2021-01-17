package net.kaaass.rumbase.parse;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 操作、标志字段的标识符
 *
 * @author kaaass
 */
@Data
@AllArgsConstructor
public class ColumnIdentifier {

    private String tableName;

    private String fieldName;
}
