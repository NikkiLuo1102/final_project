package com.luoningqi.gmall.realtime.common.bean;

public class TableProcessDim {
    //来源表名
    String sourceTable;
    //目标表名
    String sinkTable;
    String sinkColumns;
    String sinkFamily;
    String sinkRowKey;
    String op;

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public String getSinkFamily() {
        return sinkFamily;
    }

    public void setSinkFamily(String sinkFamily) {
        this.sinkFamily = sinkFamily;
    }

    public String getSinkRowKey() {
        return sinkRowKey;
    }

    public void setSinkRowKey(String sinkRowKey) {
        this.sinkRowKey = sinkRowKey;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }
}
