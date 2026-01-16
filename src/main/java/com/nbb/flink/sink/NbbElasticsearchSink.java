package com.nbb.flink.sink;

import com.nbb.flink.domain.CdcDTO;
import io.debezium.data.Envelope.Operation;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.Map;

/**
 * @author 胡鹏
 */
public class NbbElasticsearchSink {

    public static ElasticsearchSink<CdcDTO> newSink(HttpHost esHttpHost) {
        return new Elasticsearch7SinkBuilder<CdcDTO>()
                .setBulkFlushMaxActions(1)
                .setHosts(esHttpHost)
                .setEmitter(
                        (cmcDTO, context, indexer) -> {
                            Operation operation = cmcDTO.getOperation();
                            if (operation.equals(Operation.READ)
                                || operation.equals(Operation.CREATE)
                                || operation.equals(Operation.UPDATE)) {
                                IndexRequest indexRequest = createIndexRequest(cmcDTO);
                                indexer.add(indexRequest);
                            } else if (operation.equals(Operation.DELETE)) {
                                DeleteRequest deleteRequest = createDeleteRequest(cmcDTO);
                                indexer.add(deleteRequest);
                            }
                        })
                .build();
    }


    private static IndexRequest createIndexRequest(CdcDTO cdcDTO) {
        String indexName = cdcDTO.getTableName();
        Map<String, Object> source = cdcDTO.getAfter();
        String id = String.valueOf(source.get("id"));

        return Requests.indexRequest()
                .index(indexName)
                .id(id)
                .source(source);
    }

    private static DeleteRequest createDeleteRequest(CdcDTO cdcDTO) {
        String indexName = cdcDTO.getTableName();
        Map<String, Object> source = cdcDTO.getBefore();
        String id = String.valueOf(source.get("id"));

        return Requests.deleteRequest(indexName)
                .id(id);
    }
}
