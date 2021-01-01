package net.kaaass.rumbase.exception;

/**
 * 错误基类
 * <p>
 * 错误分为主错误号、子错误号。
 * 主错误号应该体现错误的类型，由子类构造器直接赋值。主错误号由模块号确定十进制千位，其余位由模块编码。
 * 子错误号体现错误的细节、逻辑。在具体抛出错误的时候赋值。
 * <p>
 * 主错误号模块分配：
 * <table>
 * <thead><tr><th>功能</th><th>包</th><th>起始主错误号</th></tr></thead>
 * <tbody>
 *     <tr><td>SQL 语句解析</td><td>parse</td><td>1000</td></tr>
 *     <tr><td>查询执行、优化</td><td>query</td><td>2000</td></tr>
 *     <tr><td>系统内数据库、表管理</td><td>table</td><td>3000</td></tr>
 *     <tr><td>索引结构，使用 B+ 树</td><td>index</td><td>4000</td></tr>
 *     <tr><td>记录管理，实现 MVCC</td><td>record</td><td>5000</td></tr>
 *     <tr><td>实现事务的管理与 2PL</td><td>transaction</td><td>6000</td></tr>
 *     <tr><td>数据项管理</td><td>dataitem</td><td>7000</td></tr>
 *     <tr><td>日志与恢复管理</td><td>recovery</td><td>8000</td></tr>
 *     <tr><td>缓冲与页管理</td><td>page</td><td>9000</td></tr>
 * </tbody>
 * </table>
 * <p>
 * 如对于 {@link net.kaaass.rumbase.record.exception.RecordNotFoundException}，主错误号
 * 是5001，而子错误号不同表示不同发生原因。如物理记录不存在，或记录对事务不可见等等。
 */
public class RumbaseException extends Exception {

    private int mainId;

    private int subId;

    /**
     * 构造Rumbase异常
     *
     * @param mainId 主错误号
     * @param subId  子错误号
     * @param reason 错误原因
     */
    public RumbaseException(int mainId, int subId, String reason) {
        super(String.format("E%d-%d: %s", mainId, subId, reason));
        this.mainId = mainId;
        this.subId = subId;
    }
}
