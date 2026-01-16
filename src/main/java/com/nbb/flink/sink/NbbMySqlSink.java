package com.nbb.flink.sink;

import com.nbb.flink.domain.CdcDTO;
import com.nbb.flink.util.SqlUtil;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author 胡鹏
 */
@Slf4j
public class NbbMySqlSink extends RichSinkFunction<CdcDTO> {

    private String jdbcUrl;
    private String username;
    private String password;
    private Connection connection;

    public NbbMySqlSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(CdcDTO cdcDTO, Context context) throws Exception {
        Statement statement = connection.createStatement();
        try {
            this.doInvoke(cdcDTO, statement);
        } catch (BatchUpdateException e) {
            if (!e.getMessage().matches("Table.*doesn't exist")) {
                log.error(e.getMessage(), e);
                return;
            }

            String createTableSql = SqlUtil.genCreateTableSql(cdcDTO);
            statement.addBatch(createTableSql);

            this.doInvoke(cdcDTO, statement);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private void doInvoke(CdcDTO cdcDTO, Statement statement) throws SQLException {
        List<String> sqlList = this.genSql(cdcDTO);
        if (CollectionUtils.isEmpty(sqlList)) {
            return;
        }
        for (String sql : sqlList) {
            statement.addBatch(sql);
        }
        statement.executeBatch();
    }

    public List<String> genSql(CdcDTO value) {
        Envelope.Operation operation = value.getOperation();
        String tableName = value.getTableName();
        Map<String, Object> before = value.getBefore();
        Map<String, Object> after = value.getAfter();

        if (operation == Envelope.Operation.READ || operation == Envelope.Operation.CREATE) {
            // 原始数据，新增数据事件
            String insertSql = SqlUtil.genInsertSql(tableName, after);
            return Collections.singletonList(insertSql);
        } else if (operation == Envelope.Operation.UPDATE) {
            // 更新数据事件
            String deleteSql = SqlUtil.genDeleteSql(tableName, "id", before.get("id").toString());
            String insertSql = SqlUtil.genInsertSql(value.getTableName(), value.getAfter());
            return Arrays.asList(deleteSql, insertSql);
        } else if (operation == Envelope.Operation.DELETE) {
            // 删除数据事件
            String deleteSql = SqlUtil.genDeleteSql(tableName, "id", before.get("id").toString());
            return Collections.singletonList(deleteSql);
        }
        return Collections.emptyList();
    }
}
