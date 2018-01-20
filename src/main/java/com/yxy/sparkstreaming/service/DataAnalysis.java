package com.yxy.sparkstreaming.service;

import com.yxy.sparkstreaming.dao.DataDaoImpl;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.sql.*;
import java.util.ArrayList;

import static com.yxy.sparkstreaming.dao.DataDaoImpl.connPool;

/**
 * @Author: Xinyu Yu
 * @Description: 数据处理与分析
 * @Date: 10:09 2018/1/17
 */
public class DataAnalysis {

    private static DataDaoImpl dataDao = new DataDaoImpl();

    private static int getCurrentNum(Connection conn) {

        long batch = DataDaoImpl.getMaxBatch(conn);
        return DataDaoImpl.getCurrentNum(batch, conn);

    }

    private static void analysis(Connection conn) {

        int currentNum = getCurrentNum(conn);
        float jumpRate = getJumpRate(conn);
        float deepRate = getDeepRate(conn);
        int newNum = getNewNum(conn);
        int oldNum = getOldNum(conn);
        updateStayTime(conn);
        updateCycle(conn);
        updateOut(conn);

        addData(currentNum, jumpRate, deepRate, newNum, oldNum, conn);

    }

    private static void updateStayTime(Connection conn) {
        DataDaoImpl.updateStayTime(conn);
    }

    private static int getNewNum(Connection conn) {
        return DataDaoImpl.getNew(conn);
    }

    private static int getOldNum(Connection conn) {
        return DataDaoImpl.getOld(conn);
    }

    private static void updateCycle(Connection conn) {
        DataDaoImpl.updateCycle(conn);
    }

    private static float getDeepRate(Connection conn) {

        long batch = DataDaoImpl.getMaxBatch(conn);

        if (batch >= 3) {
            ArrayList<String> beforeLast = DataDaoImpl.getNBatchMac(batch - 2, conn);
            ArrayList<String> lastMac = DataDaoImpl.getNBatchMac(batch - 1, conn);
            ArrayList<String> nowMac = DataDaoImpl.getNBatchMac(batch, conn);

            int numIn = 0;

            if (!beforeLast.isEmpty()) {
                for (String b : beforeLast) {
                    for (String l : lastMac) {
                        for (String n : nowMac) {
                            if (b.equals(l) && b.equals(n)) {
                                ++numIn;
                            }
                        }
                    }
                }
                return (float)numIn / beforeLast.size();
            }
        }

        return 0;

    }

    private static void updateOut(Connection conn) {

        long batch = DataDaoImpl.getMaxBatch(conn);

        if (batch >= 2) {
            ArrayList<String> lastMac = DataDaoImpl.getNBatchMac(batch - 1, conn);
            ArrayList<String> nowMac = DataDaoImpl.getNBatchMac(batch, conn);


            if (!lastMac.isEmpty()) {

                boolean in;

                for (String l : lastMac) {
                    in = false;
                    for (String n : nowMac) {
                        if (l.equals(n)) {
                            in = true;
                            break;
                        }
                    }
                    if (!in) {
                        DataDaoImpl.setZero(l, conn);
                    }
                }
            }
        }

    }

    private static float getJumpRate(Connection conn) {

        long batch = DataDaoImpl.getMaxBatch(conn);

        if (batch >= 2) {
            ArrayList<String> lastMac = DataDaoImpl.getNBatchMac(batch - 1, conn);
            ArrayList<String> nowMac = DataDaoImpl.getNBatchMac(batch, conn);

            int numIn = 0;

            if (!lastMac.isEmpty()) {
                for (String l : lastMac) {
                    for (String n : nowMac) {
                        if (l.equals(n)) {
                            ++numIn;
                        }
                    }
                }
                return (float)(lastMac.size() - numIn) / lastMac.size();
            }
        }

        return  0;

    }

    public static void getData(JavaDStream<String> a) {

        a.foreachRDD(x -> x.foreachPartition(y -> {

            Connection conn = connPool.getConnection();

            long batch = DataDaoImpl.getMaxBatch(conn);

            while (y.hasNext()) {

                String[] dataSplit = getDataSplit(y.next());

                DataDaoImpl.addRecord(conn, String.valueOf(batch + 1), dataSplit[2], dataSplit[1]);

                if (!DataDaoImpl.getUser(conn, dataSplit[2])) {
                    DataDaoImpl.addUser(conn, dataSplit[2]);
                } else {
                    if (!DataDaoImpl.getIn(conn, dataSplit[2])) {
                        DataDaoImpl.updateUser(conn, dataSplit[2]);
                    }
                }
            }

            analysis(conn);

            connPool.returnConnection(conn);
        }));

//        a.foreachRDD((JavaRDD<String> x) -> {
//            List<String> myList = x.take((int)x.count());
//
//            long batch = dataDao.getRecordBatch();
//            for (Object aMyList : myList) {
//                String[] dataSplit = getDataSplit(aMyList.toString());
//                System.out.println("batch:" + batch);
//                dataDao.addRecord(batch + 1, dataSplit[2], dataSplit[1]);
//                if (!dataDao.getUser(dataSplit[2])) {
//                    dataDao.addUser(dataSplit[2]);
//                } else {
//                    dataDao.updateUser(dataSplit[2]);
//                }
//            }
//
////            analysis();
////            try {
////                connPool.refreshConnections();
////                System.out.println("refresh");
////            } catch (SQLException e) {
////                e.printStackTrace();
////            }
//        });

    }

    private static String[] getDataSplit(String dataNonFormat ) {
        String[] aa = dataNonFormat.split(",");
        for (int i = 0; i < aa.length; i++) {
            aa[i] = aa[i].replace("\"", "");
        }
        return aa;
    }

    private static void addData(int currentNum, float jumpRate, float deepRate, int newNum, int oldNum, Connection conn) {

        DataDaoImpl.addResult(currentNum, jumpRate, deepRate, newNum, oldNum, conn);

    }

}
