/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.analysys.trino.connector.hbase.api;

import com.analysys.trino.connector.hbase.connection.HBaseClientManager;
import com.analysys.trino.connector.hbase.query.HBaseRecordSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordPageSource;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * HBase delete function interface.
 * Uncle Drew will not show up.
 * Created by wupeng on 2018/9/4.
 */
public class HBaseUpdatablePageSource implements UpdatablePageSource {
    private static final Logger log = Logger.get(HBaseUpdatablePageSource.class);
    private final HBaseClientManager clientManager;
    private String schemaName;
    private String tableName;
    private final RecordPageSource inner;

    public HBaseUpdatablePageSource(HBaseRecordSet recordSet, HBaseClientManager clientManager) {
        this.schemaName = recordSet.getHBaseSplit().getSchemaName();
//        log.info("HBaseUpdatablePageSource-----schemaName------->{"+schemaName.toString()+"}");
        this.tableName = recordSet.getHBaseSplit().getTableName();
//        log.info("HBaseUpdatablePageSource-----tableName------->{"+tableName.toString()+"}");
        this.inner = new RecordPageSource(recordSet);

        this.clientManager = clientManager;
    }

    @Override
    public void deleteRows(Block rowIds) {
        log.info("进入删除方法----->{"+rowIds.toString()+"}");
        try (Connection conn = clientManager.createConnection();
             Table table = conn.getTable(TableName.valueOf(schemaName + ":" + tableName))) {
            List<Delete> deletes = new ArrayList<>();
            Delete delete;
            for (int i = 0; i < rowIds.getPositionCount(); i++) {
                int len = rowIds.getSliceLength(i);
                Slice slice = rowIds.getSlice(i, 0, len);
                delete = new Delete(slice.getBytes());
                deletes.add(delete);
            }
            if (deletes.size() > 0)
                table.delete(deletes);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        CompletableFuture<Collection<Slice>> cf = new CompletableFuture<>();
        cf.complete(Collections.emptyList());
        return cf;
    }

    @Override
    public long getCompletedBytes() {
        return inner.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos() {
        return inner.getReadTimeNanos();
    }

    @Override
    public boolean isFinished() {
        return inner.isFinished();
    }

    @Override
    public Page getNextPage() {
        return inner.getNextPage();
    }

    @Override
    public long getMemoryUsage() {
        return inner.getMemoryUsage();
    }


    @Override
    public void close() {
        inner.close();
    }
}
