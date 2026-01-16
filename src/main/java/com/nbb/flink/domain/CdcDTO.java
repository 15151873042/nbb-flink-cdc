package com.nbb.flink.domain;

import io.debezium.data.Envelope;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * mysql表中一行数据变更的封装类
 * @author 胡鹏
 */
@Data
public class CdcDTO implements Serializable {

    /** 数据库名称 */
    private String databaseName;

    /** 表名 */
    private String tableName;

    /** 操作类型：原始数据、新增、更新、删除（原始数据为READ）  */
    private Envelope.Operation operation;

    /** 变更前的数据行信息 */
    private Map<String, Object> before;

    /** 变更后的数据行信息 */
    private Map<String, Object> after;
}
