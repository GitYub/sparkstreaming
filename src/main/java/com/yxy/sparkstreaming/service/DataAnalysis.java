package com.yxy.sparkstreaming.service;

import com.yxy.sparkstreaming.dao.DataDaoImpl;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Xinyu Yu
 * @Description: 数据处理与分析
 * @Date: 10:09 2018/1/17
 */
public class DataAnalysis {

    private static DataDaoImpl dataDao = new DataDaoImpl();

    public static void getData(JavaDStream<String> lines) {

        long batch = dataDao.getRecordBatch();
        addContent(batch + 1, lines);
        analysis();

    }

    private static int getCurrentNum() {

        long batch = dataDao.getRecordBatch();
        return dataDao.getCurrentNum(batch);

    }

    private static void analysis() {

        int currentNum = getCurrentNum();
        float jumpRate = getJumpRate();
        float deepRate = getDeepRate();
        int newNum = getNewNum();
        int oldNum = getOldNum();

        addData(currentNum, jumpRate, deepRate, newNum, oldNum);


    }

    private static int getNewNum() {
        return dataDao.getNew();
    }

    private static int getOldNum() {
        return dataDao.getOld();
    }



    private static float getDeepRate() {

        long batch = dataDao.getRecordBatch();

        if (batch >= 3) {
            ArrayList<String> beforeLast = dataDao.getNBatchMac(batch - 2);
            ArrayList<String> lastMac = dataDao.getNBatchMac(batch - 1);
            ArrayList<String> nowMac = dataDao.getNBatchMac(batch);

            int numIn = 0;

            if (!beforeLast.isEmpty()) {
                for (String b : beforeLast) {
                    for (String l : lastMac) {
                        for (String n : nowMac) {
                            if (b.equals(l) && b.equals(n)) {
                                ++numIn;
                                lastMac.remove(l);
                                nowMac.remove(n);
                            }
                        }
                    }
                }
                return numIn / beforeLast.size() * 100;
            }
        }

        return 0;

    }

    private static float getJumpRate() {

        long batch = dataDao.getRecordBatch();

        if (batch >= 2) {
            ArrayList<String> lastMac = dataDao.getNBatchMac(batch - 1);
            ArrayList<String> nowMac = dataDao.getNBatchMac(batch);

            int numIn = 0;

            if (!lastMac.isEmpty()) {
                for (String l : lastMac) {
                    for (String n : nowMac) {
                        if (l.equals(n)) {
                            ++numIn;
                            nowMac.remove(n);
                        }
                    }
                }
                return (lastMac.size() - numIn) / lastMac.size() * 100;
            }
        }

        return  0;

    }

    private static void addContent(long batch, JavaDStream<String> a) {
        a.foreachRDD((JavaRDD<String> x) -> {
            List<String> myList = x.take((int)x.count());

            for (Object aMyList : myList) {
                String[] dataSplit = getDataSplit(aMyList.toString());
                dataDao.addRecord(batch, dataSplit[2], dataSplit[1]);
//                if (!dataDao.getUser(dataSplit[2])) {
//                    dataDao.addUser(dataSplit[2]);
//                } else {
//
//                }
            }
        });
    }

    private static String[] getDataSplit(String dataNonFormat ) {
        String[] aa = dataNonFormat.split(",");
        for (int i = 0; i < aa.length; i++) {
            aa[i] = aa[i].replace("\"", "");
        }
        return aa;
    }

    private static void addData(int currentNum, float jumpRate, float deepRate, int newNum, int oldNum) {

        dataDao.addResult(currentNum, jumpRate, deepRate, newNum, oldNum);

    }

}
