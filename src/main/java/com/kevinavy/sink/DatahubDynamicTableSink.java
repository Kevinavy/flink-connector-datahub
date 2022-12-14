package com.kevinavy.sink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class DatahubDynamicTableSink implements DynamicTableSink {
    private final String endpoint;
    private final String accessId;
    private final String accessKey;
    private final String project;
    private final String topic;
    private final Integer shard;
    private final Integer lifecycle;

    public DatahubDynamicTableSink(String endpoint,
                                   String accessId,
                                   String accessKey,
                                   String project,
                                   String topic,
                                   Integer shard,
                                   Integer lifecycle) {
        this.endpoint = endpoint;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.project = project;
        this.topic = topic;
        this.shard = shard;
        this.lifecycle = lifecycle;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.upsert();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        final SinkFunction<RowData> sinkFunction = new DatahubSinkFunction(
                endpoint,
                accessId,
                accessKey,
                project,
                topic,
                shard,
                lifecycle);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new DatahubDynamicTableSink(endpoint, accessId, accessKey, project, topic, shard, lifecycle);

    }

    @Override
    public String asSummaryString() {
        return "set up datahub connector";
    }
}
