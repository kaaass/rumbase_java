package net.kaaass.rumbase;

import java.io.File;

/**
 * 单元测试常见文件操作
 */
public class FileUtil {
    public final static String DATA_PATH = "data/";
    public final static String TABLE_PATH = "data/table/";
    public static final String TEST_PATH = "test_gen_files/";

    public static void createDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            FileUtil.removeDir(dir);
        }
        assert dir.mkdirs();
    }

    public static void removeDir(String path) {
        removeDir(new File(path));
    }

    public static void removeDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    removeDir(file);
                } else {
                    assert file.delete();
                }
            }
        }
        dir.delete();
    }
}
