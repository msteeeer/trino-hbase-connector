package com.analysys.trino.connector.hbase.api;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @description:
 * @author: wyz
 * @createDate: 2023-08-09 10:03
 * @version: 1.0
 */
public interface UpdatablePageSource  extends ConnectorPageSource {

    void deleteRows(Block rowIds);

    CompletableFuture<Collection<Slice>> finish();

    default void abort() {}



}
