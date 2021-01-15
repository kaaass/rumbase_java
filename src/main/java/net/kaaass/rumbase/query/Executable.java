package net.kaaass.rumbase.query;

import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;

/**
 * 执行接口，实现该接口可以接受Statement语句并执行
 *
 * @author @KveinAxel
 */
public interface Executable {

    /**
     * 执行器的执行接口
     */
    void execute() throws TableExistenceException, IndexAlreadyExistException, TableConflictException, ArgumentException;
}
