package com.yxy.sparkstreaming.dao;

import com.yxy.sparkstreaming.Utils.JdbcUtil;

import javax.validation.constraints.Null;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataDaoImpl implements DataDao {

    @Override
    public void addRecord(long batch, String mac, String time) {

        try {
            Connection conn = JdbcUtil.getConnection();

            String sql = "insert into record(batch, mac, current_time) values("
                    + String.valueOf(batch) + ", '" + mac + "', '" + time +"')";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public int getCurrentNum(long batch) {

        int num = 0;
        try {
            Connection conn = JdbcUtil.getConnection();
            String sql = "select count(*) from record where batch = " + String.valueOf(batch);

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                batch = resultSet.getLong(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;

    }

    public boolean getUser(String mac) {

        int num = 0;
        try {
            Connection conn = JdbcUtil.getConnection();
            String sql = "select count(*) from user where mac ='" + mac + "'";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                num = resultSet.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num != 0;

    }

    public void addUser(String mac) {

        try {
            Connection conn = JdbcUtil.getConnection();

            String sql = "insert into user(mac, is_in) values('"
                    + mac + "', " + 1 +")";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void updateUser(String mac, String time) {

        try {
            Connection conn = JdbcUtil.getConnection();

            String sql = "update user user(is_in, last_time) values('"
                    + mac + "', " + 1 +")";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public long getRecordBatch() {

        long batch = 0;
        try {
            Connection conn = JdbcUtil.getConnection();
            String sql = "select max(batch) from record";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                batch = resultSet.getLong(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return batch;

    }

    public ArrayList<String> getNBatchMac(long batch) {

        ArrayList<String> mac = new ArrayList<>();

        try {
            Connection conn = JdbcUtil.getConnection();
            String sql = "select mac from record where batch = " + batch;

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                mac.add(resultSet.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return mac;

    }

    private String getTimestamp() {

        String timestamp = "";

        try {
            Connection conn = JdbcUtil.getConnection();
            String sql = "select now()";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                timestamp = resultSet.getString(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return timestamp;

    }

    public int getNew() {

        int num = 0;
        try {
            Connection conn = JdbcUtil.getConnection();
            String sql = "select count(*) from user where last_time = ''";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                num = resultSet.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return num;

    }

    public int getOld() {

        int num = 0;
        try {
            Connection conn = JdbcUtil.getConnection();
            String sql = "select count(*) from user where is_in = 1 and last_time != ''";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                num = resultSet.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return num;

    }

    public void addResult(int current_in, float jump_rate, float deep_rate, int newNum, int oldNum) {

        try {
            Connection conn = JdbcUtil.getConnection();

            String sql = "insert into data(mytimestamp, current_in, jump_rate, deep_rate, new_num, old_num) values('"
                    + getTimestamp() + "', " + current_in + jump_rate + deep_rate + ")";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
