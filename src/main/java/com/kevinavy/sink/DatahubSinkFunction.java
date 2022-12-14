package com.kevinavy.sink;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.model.*;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Streams;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DatahubSinkFunction extends RichSinkFunction<RowData> {
    private final String endpoint;
    private final String accessId;
    private final String accessKey;
    private final String project;
    private final String topic;
    private final Integer shard;
    private final Integer lifecycle;

    private DatahubClient datahubClient;

    private final int batch = 10;
    List<GenericRowData> rowDataList = new ArrayList<>();
    private int count;

    public DatahubSinkFunction(String endpoint,
                               String accessId,
                               String accessKey,
                               String project,
                               String topic,
                               Integer shard,
                               Integer lifecycle
    ) {
        this.endpoint = endpoint;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.project = project;
        this.topic = topic;
        this.shard = shard;
        this.lifecycle = lifecycle;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        datahubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(endpoint,
                                new AliyunAccount(accessId, accessKey), true))
                .build();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void finish() throws Exception {
        super.finish();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        GenericRowData rowData = (GenericRowData) value;
        RowKind rowKind = rowData.getRowKind();
        if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
            this.rowDataList.add(rowData);
            this.count++;
            if (count >= batch) {
                writeToTopic(this.project, this.topic, 10, rowDataList);
            }
        }
    }

    // 写入Tuple型数据
    public void writeToTopic(String project, String topic, int retryTimes, List<GenericRowData> rowDataList) throws Exception {
        // 获取schema
        RecordSchema recordSchema = datahubClient.getTopic(project, topic).getRecordSchema();
        int arity = rowDataList.get(0).getArity();
        if (arity != recordSchema.getFields().size()) {
            throw new RuntimeException("schema not match");
        }

        List<RecordEntry> recordEntries = Streams.mapWithIndex(rowDataList.stream(), (rowData, index) -> {
            RecordEntry recordEntry = new RecordEntry();
            // 字段映射
            TupleRecordData data = new TupleRecordData(recordSchema);
            for (int i = 0; i < recordSchema.getFields().size(); i++) {
                Object field = rowData.getField(i);
                data.setField(recordSchema.getField(i).getName(), field == null ? null : field.toString());
            }
            recordEntry.setRecordData(data);
            return recordEntry;
        }).collect(Collectors.toList());

        try {
            PutRecordsResult result = datahubClient.putRecords(project, topic, recordEntries);
            int i = result.getFailedRecordCount();
            if (i > 0) {
                retry(datahubClient, result.getFailedRecords(), retryTimes, project, topic);
            }
            else {
                this.rowDataList = new ArrayList<>();
                this.count = 0;
            }
        }  catch (DatahubClientException e) {
            System.out.println("requestId:" + e.getRequestId() + "\tmessage:" + e.getErrorMessage());
        }
    }
    //重试机制
    public static void retry(DatahubClient client, List<RecordEntry> records, int retryTimes, String project, String topic) {
        boolean suc = false;
        while (retryTimes != 0) {
            retryTimes = retryTimes - 1;
            PutRecordsResult recordsResult = client.putRecords(project, topic, records);
            if (recordsResult.getFailedRecordCount() > 0) {
                retry(client,recordsResult.getFailedRecords(),retryTimes,project,topic);
            }
            suc = true;
            break;
        }
        if (!suc) {
            System.out.println("retryFailure");
        }
    }

}
