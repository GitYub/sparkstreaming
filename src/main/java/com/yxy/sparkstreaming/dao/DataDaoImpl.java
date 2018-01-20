package com.yxy.sparkstreaming.dao;

import com.yxy.sparkstreaming.pool.ConnectionPool;

import java.sql.*;
import java.util.ArrayList;

/**
 * @Author: Xinyu Yu
 * @Description: 数据库操作
 * @Date: 17:50 2018/1/20
 */
public class DataDaoImpl {

    public static ConnectionPool connPool = new ConnectionPool(
            "com.mysql.jdbc.Driver",
            "jdbc:mysql://120.79.139.121:3306/BA?characterEncoding=utf8&useSSL=true",
            "root",
            "2254655"
    );

    public DataDaoImpl() {

        try {
            connPool.createPool();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void addRecord(Connection conn, String batch, String mac, String currentTime) {

        String sql = "insert into record(batch, mac, insert_current_time) values("
                + batch + ", '"   + mac + "', '" + currentTime +"')";

        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();

            preparedStatement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static long getMaxBatch(Connection conn) {

        String sql = "select max(batch) from record";

        long batch = 0;
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                batch = resultSet.getLong(1);
            }

            preparedStatement.close();
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return batch;

    }

    public static boolean getUser(Connection conn, String mac) {

        String sql = "select count(*) from user where mac = '" + mac + "'";

        int num = 0;
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                num = resultSet.getInt(1);
            }

            preparedStatement.close();
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num != 0;

    }

    public static void addUser(Connection conn, String mac) {

        String sql = "insert into user(mac, is_in, in_time) values('"
                + mac + "', 1, unix_timestamp(now()))";

        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();

            preparedStatement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static int getCurrentNum(long batch, Connection conn) {

        int num = 0;
        try {

            String sql = "select count(*) from record where batch = " + String.valueOf(batch);

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                num = resultSet.getInt(1);
            }

            preparedStatement.close();
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;

    }



    public static void setZero(String mac, Connection conn) {

        try {

            if (!getIn(conn, mac)) {
                String sql = "update user set is_in = 0, last_time = in_time where mac = '" + mac + "'";
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                preparedStatement.executeUpdate();
                preparedStatement.close();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static boolean getIn(Connection conn, String mac) {

        String sql = "select is_in from user where mac = '" + mac + "'";

        int num = 0;
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                num = resultSet.getInt(1);
            }

            preparedStatement.close();
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return num != 0;
    }

    public static void updateUser(Connection conn, String mac) {

        String sql = "update user set is_in = 1, times = times + 1, in_time = unix_timestamp(now()) where mac = '" + mac + "'";

        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();

            preparedStatement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void updateStayTime(Connection conn) {

        try {

            String sql = "update user set stay_time = unix_timestamp(now()) - in_time where is_in = 1";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();
            preparedStatement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static ArrayList<String> getNBatchMac(long batch, Connection conn) {

        ArrayList<String> mac = new ArrayList<>();

        try {

            String sql = "select mac from record where batch = " + batch;

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                mac.add(resultSet.getString(1));
            }

            preparedStatement.close();
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return mac;

    }

    public static void updateCycle(Connection conn) {

        try {

            String sql = "update user set cycle = unix_timestamp(in_time) - unix_timestamp(last_time) where is_in = 1 and times != 1";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();

            preparedStatement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static String getTimestamp(Connection conn) {

        String timestamp = "";

        try {

            String sql = "select unix_timestamp(now())";

            PreparedStatement preparedStatement;

                preparedStatement = conn.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    timestamp = resultSet.getString(1);
                }
                preparedStatement.close();
                resultSet.close();

            } catch (SQLException e) {
            e.printStackTrace();
        }

        return timestamp;

    }

    public static int getNew(Connection conn) {

        int num = 0;
        try {

            String sql = "select count(*) from user where last_time = ''";


            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                num = resultSet.getInt(1);
            }
            preparedStatement.close();
            resultSet.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return num;

    }

    public static int getOld(Connection conn) {

        int num = 0;
        try {

            String sql = "select count(*) from user where is_in = 1 and last_time != ''";

            if (conn != null) {
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    num = resultSet.getInt(1);
                }
                preparedStatement.close();
                resultSet.close();

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return num;

    }

    public static void addResult(int current_in, float jump_rate, float deep_rate, int newNum, int oldNum, Connection conn) {

        try {

            String sql = "insert into data(mytimestamp, current_in, jump_rate, deep_rate, new_num, old_num) values('"
                    + getTimestamp(conn) + "', " + current_in + ", " + jump_rate + ", " + deep_rate + ", " + newNum + ", " + oldNum + ")";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();
            preparedStatement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
