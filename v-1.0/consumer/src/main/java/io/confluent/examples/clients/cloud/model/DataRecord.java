package io.confluent.examples.clients.cloud.model;

public class DataRecord {

    Double count;

    public DataRecord() {
    }

    public DataRecord(Double count) {
        this.count = count;
    }

    public Double getCount() {
        return count;
    }

    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }

}
