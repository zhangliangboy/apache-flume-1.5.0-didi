package com.kuaidadi;

/**
 * Error 分析工具类启动类
 * 
 * @author zhao qun modified by zhangliang
 * @version $Id: ErrorToolLauncher.java, v 0.1 Dec 1, 2014 8:26:21 PM zhangliang
 *          Exp $
 */
public class ErrorToolLauncher {

    public static void main(String args[]) {

        ErrorAnalyzeAvroSourceTool errorAnalyzeAvroSourceTool = new ErrorAnalyzeAvroSourceTool();
        try {
            errorAnalyzeAvroSourceTool.start();
        } finally {
        }
    }
}