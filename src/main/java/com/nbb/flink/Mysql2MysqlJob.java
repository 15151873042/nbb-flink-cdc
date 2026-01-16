package com.nbb.flink;

import com.nbb.flink.domain.CdcDTO;
import com.nbb.flink.schema.NbbDeserializationSchema;
import com.nbb.flink.sink.NbbMySqlSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 胡鹏
 */
public class Mysql2MysqlJob {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        // 设置任务名称
        config.set(PipelineOptions.NAME, "mysql同步到mysql测试任务");
        // 设置本地WebUI的端口为8081
        config.set(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // FIXME 库同步，全量 + 增量同步时，必须开启checkpoint，否则增量数据无法获取
        env.enableCheckpointing(10000L);

        MySqlSource<CdcDTO> mySqlSource = MySqlSource.<CdcDTO>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .serverTimeZone("Asia/Shanghai")
                .databaseList("flink_cdc_source")
                .tableList("flink_cdc_source.*")
                .deserializer(new NbbDeserializationSchema())
                // 整个库同步，全量 + 增量
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<CdcDTO> mysqlDataSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "myqlSource");

        mysqlDataSource.addSink(new NbbMySqlSink("jdbc:mysql://127.0.0.1:3306/flink_cdc_sink", "root", "root"));
        mysqlDataSource.print();

        env.execute();
    }
}
