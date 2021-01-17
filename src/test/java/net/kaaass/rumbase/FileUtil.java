package net.kaaass.rumbase;

import java.io.File;

/**
 * 单元测试常见文件操作
 */
public class FileUtil {
    public final static String DATA_PATH = "data";
    public final static String PATH = "data/table/";

    public static void removeDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    removeDir(file);
                } else {
                    file.delete();
                }
            }
        }

        dir.delete();
    }
}
