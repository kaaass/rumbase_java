package net.kaaass.rumbase.transaction.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 有向图数据结构
 * <p>
 * 用于死锁检测
 *
 * @author criki
 */
public class Graph {

    /**
     * 深搜访问数组
     */
    private final Map<Integer, Integer> visited = new HashMap<>();
    /**
     * 等待图邻接表
     */
    public Map<Integer, List<Integer>> waitGraph = new HashMap<>();

    /**
     * 添加边
     *
     * @param from 源点
     * @param to   终点
     */
    public void addEdge(int from, int to) {
        if (waitGraph.containsKey(from)) {
            waitGraph.get(from).add(to);
        } else {
            List<Integer> edges = new ArrayList<>();
            edges.add(to);
            waitGraph.put(from, edges);
        }

        if (!waitGraph.containsKey(to)) {
            List<Integer> edges = new ArrayList<>();
            waitGraph.put(to, edges);
        }
    }

    /**
     * 判断是否存在环路
     *
     * @return <table>
     * <tr><td>true</td><td>存在环路</td></tr>
     * <tr><td>false</td><td>不存在环路</td></tr>
     * </table>
     */
    public boolean hasLoop() {
        // 初始化访问数组
        visited.clear();
        for (Integer vertex : waitGraph.keySet()) {
            visited.put(vertex, 0);
        }

        for (Map.Entry<Integer, Integer> v : visited.entrySet()) {
            if (v.getValue() == 0) {
                if (dfs(v.getKey())) {
                    // 有环
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 从u点开始深度优先搜索
     *
     * @param u 源点
     * @return 是否存在环路
     */
    private boolean dfs(int u) {
        // 标识正在访问
        visited.put(u, -1);

        List<Integer> edges = waitGraph.get(u);
        for (Integer v : edges) {
            int visitedV = visited.get(v);
            if (visitedV < 0) {
                return true;
            } else if (visitedV == 0 && dfs(v)) {
                return true;
            }
        }

        visited.put(u, 1);
        return false;
    }
}
