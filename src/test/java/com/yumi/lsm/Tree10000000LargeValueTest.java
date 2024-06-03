package com.yumi.lsm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;

import static com.yumi.lsm.TreeTestHelper.cleanFolder;

public class Tree10000000LargeValueTest {
    @Test
    public void testAdd() {
        String ww = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        String workDir = "/tmp/yumi";
        File file = new File(workDir);
        cleanFolder(file);
        file.delete();

        Tree tree = new Tree(Config.newConfig(workDir));
        for (int i = 0; i < 10000000; i++) {
            tree.put(("testKey" + i).getBytes(), (i + ww).getBytes());
            if (i % 1000 == 0) {
                System.out.println("testKey" + i);
            }
        }
        tree.close();
    }

    @Test
    public void testRead() {
        String workDir = "/tmp/yumi";
        Tree tree = new Tree(Config.newConfig(workDir));
        int i = 0;
        for (; i < 1000; i++) {
            String key ="testKey" + new Random().nextInt(10000000);
            byte[] value = tree.get(key.getBytes());
            if (null != value) {
                System.out.println(new String(value));
            } else {
                System.out.println(key);
                break;
            }
        }
        tree.close();
        Assertions.assertEquals(1000, i);
    }
}
