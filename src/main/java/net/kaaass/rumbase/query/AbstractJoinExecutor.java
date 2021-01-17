package net.kaaass.rumbase.query;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.stmt.SelectStatement;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.List;

/**
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public abstract class AbstractJoinExecutor implements Executable {

    @NonNull
    protected final String fromTable;

    @NonNull
    protected final List<SelectStatement.JoinTable> joins;

    @NonNull
    protected final TableManager manager;

    @NonNull
    protected final TransactionContext context;

    @Getter
    protected List<ColumnIdentifier> resultIdr;

    @Getter
    protected List<List<Object>> resultRows;

}
