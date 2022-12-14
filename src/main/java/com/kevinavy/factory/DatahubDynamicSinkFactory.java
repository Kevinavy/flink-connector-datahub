package com.kevinavy.factory;

import com.kevinavy.sink.DatahubDynamicTableSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class DatahubDynamicSinkFactory implements DynamicTableSinkFactory {
    public static final ConfigOption<String> ENDPOINT = ConfigOptions.key("endpoint")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ACCESS_ID = ConfigOptions.key("access-id")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ACCESS_KEY = ConfigOptions.key("access-key")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> TOPIC = ConfigOptions.key("topic")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PROJECT = ConfigOptions.key("project")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Integer> SHARD = ConfigOptions.key("topic.shard")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> LIFECYCLE = ConfigOptions.key("topic.lifecycle")
            .intType()
            .defaultValue(7);

    @Override
    public String factoryIdentifier() {
        return "datahub";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ENDPOINT);
        options.add(ACCESS_ID);
        options.add(ACCESS_KEY);
        options.add(TOPIC);
        options.add(PROJECT);
//        options.add(FactoryUtil.FORMAT); // ????????????????????????????????????????????????
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SHARD);
        options.add(LIFECYCLE);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        // ???????????????????????????????????????????????????????????????
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // ????????????????????????
//        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
//                SerializationFormatFactory.class,
//                FactoryUtil.FORMAT);

        // ????????????????????????
        helper.validate();

        // ???????????????????????????
        final ReadableConfig options = helper.getOptions();
        final String endpoint = options.get(ENDPOINT);
        final String accessKey = options.get(ACCESS_KEY);
        final String accessId = options.get(ACCESS_ID);
        final String project = options.get(PROJECT);
        final String topic = options.get(TOPIC);
        final Integer lifecycle = options.get(LIFECYCLE);
        final Integer shard = options.get(SHARD);

//        // ??? catalog ????????????????????????????????? (????????????????????????)
//        final DataType producedDataType =
//                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // ???????????????????????? source
        return new DatahubDynamicTableSink(endpoint, accessId, accessKey, project, topic, shard, lifecycle);
    }
}
