package com.yumi.lsm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

import static com.yumi.lsm.TreeTestHelper.cleanFolder;

public class TreeBaseTest {
    @Test
    public void testBaseUseCases() {
        String workDir = "/tmp/yumi";
        File file = new File(workDir);
        cleanFolder(file);
        file.delete();

        Config config = Config.newConfig(workDir);
        Tree tree = new Tree(config);
        byte[] bytes = tree.get("yumi1".getBytes());
        Assertions.assertNull(bytes);
        for (int i = 0; i < 100; i++) {
            tree.put(("yumi" + i).getBytes(), "yumi1".getBytes());
        }
        tree.close();

        tree = new Tree(config);
        byte[] bytes1 = tree.get("yumi99".getBytes());
        Assertions.assertEquals(new String(bytes1), "yumi1");
        for (int i = 100; i < 200; i++) {
            tree.put(("yumi" + i).getBytes(), "yumi1".getBytes());
        }
        tree.close();
    }
}
