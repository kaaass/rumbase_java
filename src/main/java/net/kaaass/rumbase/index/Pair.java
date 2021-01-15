package net.kaaass.rumbase.index;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 用于保存索引中的key-uuid对，上层模块可以根据key的值判断是否决定停止迭代，以获得想要的范围数据
 * @author 无索魏
 */
@Data
@AllArgsConstructor
public class Pair {
    private long key;
    private long uuid;
}
