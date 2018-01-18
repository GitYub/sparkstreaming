package com.yxy.sparkstreaming.dao;

public interface DataDao {
    void addRecord(long batch, String mac, String time);
    long getRecordBatch();
    //void addResult(int current_in, float jump_rate, float deep_rate);
}
