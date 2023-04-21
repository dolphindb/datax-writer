package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.plugin.writer.dolphindbwriter.DolphinDbTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

public class DolphinDbTemplateTest {
    public static void main(String[] args) {
        try {
            System.out.println("start");

            String windowcmd = "cmd /c python datax.py D:\\new\\DataXServe\\datax\\job\\job.json";
            Process pr = Runtime.getRuntime().exec(windowcmd, null, new File("D:\\new\\DataXServe\\datax\\bin"));
            BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line = null;
            while ((line=in.readLine())!=null){
                System.out.println(line);
            }
            in.close();
            pr.waitFor();

            System.out.println("end");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
