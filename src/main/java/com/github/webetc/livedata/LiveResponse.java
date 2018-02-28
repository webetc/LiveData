package com.github.webetc.livedata;

import java.util.ArrayList;
import java.util.List;

public class LiveResponse {
    public static final String Load = "L";      // empty and load new set of data
    public static final String Modify = "M";    // insert/update specific records
    public static final String Delete = "D";    // delete specific records
    public static final String Error = "E";     // error client should wipe data and restart

    private String action;
    private String schema;
    private String table;
    private int idColumnIndex = 0;
    private List<String> columns;
    private List<List<String>> records;
    public transient long largestId = 0;

    public LiveResponse(String action, String schema, String tableName) {
        this.action = action;
        this.schema = schema;
        this.table = tableName;
    }

    public String getAction() {
        return action;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public int getIdColumnIndex() {
        return idColumnIndex;
    }

    public void setIdColumnIndex(int idColumnIndex) {
        this.idColumnIndex = idColumnIndex;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void addColumn(String columnName) {
        if (columns == null)
            setColumns(new ArrayList<>());
        columns.add(columnName);
    }

    public List<List<String>> getRecords() {
        return records;
    }

    public void setRecords(List<List<String>> records) {
        this.records = records;
    }

    public void addRecord(List<String> record) {
        if (records == null)
            setRecords(new ArrayList<>());

        records.add(record);
    }
}
