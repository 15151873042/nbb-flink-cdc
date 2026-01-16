package com.nbb.flink.util;

import cn.hutool.core.util.StrUtil;
import com.nbb.flink.domain.CdcDTO;
import io.debezium.data.Envelope;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 胡鹏
 */
public class SqlUtil {

    private static final String INSERT_SQL_TEMPLATE = "insert into {}({}) values({});";

    private static final String DELETE_SQL_TEMPLATE = "delete from {} where {} = {};";



    public static String genInsertSql(String tableName, Map<String, Object> data) {
        String columnNameSegment = StrUtil.join(", ", data.keySet());

        String columnValueSegment = data.values().stream()
                .map(columnValue -> {
                    if (columnValue instanceof String) {
                        return StrUtil.wrap((String) columnValue, "'", "'");
                    } else {
                        return StrUtil.toString(columnValue);
                    }
                })
                .collect(Collectors.joining(", "));

        return StrUtil.format(INSERT_SQL_TEMPLATE, tableName, columnNameSegment, columnValueSegment);
    }


    public static String genDeleteSql(String tableName, String primaryKeyName, String primaryValueName) {
        return StrUtil.format(DELETE_SQL_TEMPLATE, tableName, primaryKeyName, primaryValueName);
    }


    public static String genCreateTableSql(CdcDTO sqlCdcDTO) {

        Envelope.Operation operation = sqlCdcDTO.getOperation();


        Map<String, Object> data;

        if (operation == Envelope.Operation.DELETE) {
            data = sqlCdcDTO.getBefore();
        } else {
            data = sqlCdcDTO.getAfter();
        }

        String tableName = sqlCdcDTO.getTableName();

        Set<String> keys = data.keySet();

        String columnSql = data.keySet().stream()
                .filter(columnName -> !columnName.equals("id"))
                .map(columnName -> columnName + " longtext default null,")
                .collect(Collectors.joining(" "));

        return "create table IF NOT EXISTS " + tableName + "( id int not null, " + columnSql + " primary key (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

    }

    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", 123456790123456789L);
        map.put("name", "张三");
        map.put("age", 10);
        map.put("deleted", true);

    }
}
