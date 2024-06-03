package com.yumi.lsm;

import java.io.File;

public class TreeTestHelper {
    public static boolean cleanFolder(File folder) {
        // 确保传入的是一个文件夹
        if (folder.isDirectory()) {
            // 获取文件夹中的所有文件和文件夹
            File[] files = folder.listFiles();
            if (files != null) {
                for (File f : files) {
                    // 如果是文件夹，则递归清空
                    if (f.isDirectory()) {
                        cleanFolder(f);
                    }
                    // 删除文件或已清空的子文件夹
                    f.delete();
                }
            }
            return true;
        } else {
            System.out.println("提供的路径不是一个文件夹。");
            return false;
        }
    }
}
