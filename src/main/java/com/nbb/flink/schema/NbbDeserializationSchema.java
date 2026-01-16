package com.nbb.flink.schema;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.nbb.flink.domain.CdcDTO;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 将Debezium（一个开源的分布式变更数据捕获）解析出来的原始变更事件，反序列化成 Flink 程序可以直接处理的 Java 对象
 * @author 胡鹏
 */
public class NbbDeserializationSchema implements DebeziumDeserializationSchema<CdcDTO> {

    static final DateTimeFormatter LOCAL_DATE_FORMATTER =   DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER =   DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<CdcDTO> collector) {

        // 获取databaseName和tableName
        List<String> databaseAndTables = StrUtil.split(sourceRecord.topic(), '.');
        String databaseName = databaseAndTables.get(1);
        String tableName = databaseAndTables.get(2);

        // 解析before
        Struct valueStruct = (Struct)sourceRecord.value();
        Struct beforeStruct = valueStruct.getStruct("before");
        Map<String, Object> beforeData = getData(beforeStruct);

        // 解析after
        Struct afterStruct = valueStruct.getStruct("after");
        Map<String, Object> afterData = getData(afterStruct);

        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        CdcDTO CdcDTO = new CdcDTO();
        CdcDTO.setDatabaseName(databaseName);
        CdcDTO.setTableName(tableName);
        CdcDTO.setOperation(operation);
        CdcDTO.setBefore(beforeData);
        CdcDTO.setAfter(afterData);

        collector.collect(CdcDTO);
    }

    @Override
    public TypeInformation<CdcDTO> getProducedType() {
        return TypeInformation.of(CdcDTO.class);
    }


    private Map<String, Object> getData(Struct struct) {
        if (ObjectUtil.isNull(struct)) {
            return Collections.emptyMap();
        }

        List<Field> fields = struct.schema().fields();
        return fields.stream()
                .collect(Collectors.toMap(
                        Field::name,
                        field -> {
                            Object originFieldValue = struct.get(field);
                            Object finalFieldValue = fieldValueTransition(field, originFieldValue);
                            return finalFieldValue;
                        }
                ));
    }


    /**
     *
     * @param field
     * @param FieldValue
     * @return
     */
    Object fieldValueTransition(Field field, Object FieldValue) {

        String fieldSchemaTypeName = field.schema().type().getName();
        String fieldSchemaName = field.schema().name();


        if ("int32".equals(fieldSchemaTypeName) && "io.debezium.time.Date".equals(fieldSchemaName)) {
            // date类型默认转换成了距离1970-01-01日的天数，例如1970-01-02会转换成1，1969-12-31会转换成-1
            LocalDate localDate = LocalDate.of(1970, 1, 1).plusDays((int)FieldValue);
            return LOCAL_DATE_FORMATTER.format(localDate);
        }

        if ("int64".equals(fieldSchemaTypeName) && "io.debezium.time.Timestamp".equals(fieldSchemaName)) {
            // datetime类型会转换成时间戳，且用的是UTC时间（多+了8个小时）
            LocalDateTime localDateTime = LocalDateTime
                    .ofEpochSecond((long)FieldValue / 1000, 0, ZoneOffset.UTC);
            return LOCAL_DATE_TIME_FORMATTER.format(localDateTime);
        }

        return FieldValue;
    }


}
