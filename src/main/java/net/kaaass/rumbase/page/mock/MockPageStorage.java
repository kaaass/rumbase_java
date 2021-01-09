package net.kaaass.rumbase.page.mock;

import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.exception.FileException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;


public class MockPageStorage implements PageStorage {


    public MockPageStorage(String filepath) throws FileException {
        this.filepath = filepath;
        File file = new File(filepath);
        if (!file.exists()) {
            try {
                file.createNewFile();
                OutputStream os = null;
                try {
                    os = new FileOutputStream(file, true);
                    byte[] data = new byte[PageManager.PAGE_SIZE];
                    for (int i = 0; i < 10; i++) {
                        os.write(data, 0, data.length);
                        os.flush();
                    }
                } catch (Exception e) {
                    throw new FileException(2);
                }
            } catch (Exception e) {
                throw new FileException(1);
            }
        }
    }

    @Override
    public Page get(long pageId) throws FileException {
        File file = new File(filepath);
        //文件会预留5页作为文件头
        try {
            FileInputStream in = new FileInputStream(file);
            byte[] data = new byte[PageManager.PAGE_SIZE];
            try {
                in.skip((pageId + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
                int readNumber = in.read(data);
                if (readNumber < PageManager.PAGE_SIZE) {
                    throw new FileException(4);
                }
            } catch (Exception e) {
                throw new FileException(4);
            }

            return new MockPage(data, pageId, this.filepath);
        } catch (Exception e) {
            throw new FileException(3);
        }
    }

    @Override
    public void flush() {
        //TODO
    }

    private String filepath;
}
