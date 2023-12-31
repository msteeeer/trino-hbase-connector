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
package com.analysys.trino.connector.hbase.schedule;

import com.analysys.trino.connector.hbase.connection.HBaseClientManager;
import com.analysys.trino.connector.hbase.frame.HBaseConnectorId;
import com.analysys.trino.connector.hbase.meta.*;
import com.analysys.trino.connector.hbase.utils.Constant;
import com.analysys.trino.connector.hbase.utils.TimeTicker;
import com.analysys.trino.connector.hbase.utils.Utils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.analysys.trino.connector.hbase.utils.Constant.*;
import static com.analysys.trino.connector.hbase.utils.Utils.isEmpty;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;

/**
 * HBase split manager
 *
 * @author wupeng
 * @date 2019/01/29
 */
public class HBaseSplitManager implements ConnectorSplitManager {

    public static final Logger log = Logger.get(HBaseSplitManager.class);

    private final String connectorId;
    private final HBaseClientManager clientManager;

    private final HBaseConfig config;

    @Inject
    public HBaseSplitManager(HBaseConnectorId connectorId, HBaseClientManager clientManager, HBaseConfig config) {
        this.connectorId = (Objects.requireNonNull(connectorId, "connectorId is null")).toString();
        this.clientManager = Objects.requireNonNull(clientManager, "client is null");
        this.config = Objects.requireNonNull(config, "config is null");
        log.info("\nPresto HBase Connector Config：" + this.config.toString());
    }


    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter, Constraint constraint1) {

        HBaseTableHandle tableHandle = (HBaseTableHandle) connectorTableHandle;

        String schemaName = tableHandle.getSchemaTableName().getSchemaName();
        String tableName = tableHandle.getSchemaTableName().getTableName();
        HBaseTable table = this.clientManager.getTable(schemaName, tableName);
        Preconditions.checkState(table != null, "Table %s.%s no longer exists", schemaName, tableName);

        TupleDomain<ColumnHandle> constraint = tableHandle.getConstraint();
        TableMetaInfo tableMetaInfo = Utils.getTableMetaInfoFromJson(schemaName, tableName, config.getMetaDir());
        Preconditions.checkState(tableMetaInfo != null,
                String.format("The meta info of table %s.%s doesn't exists! Table meta dir is %s.",
                        schemaName, tableName, config.getMetaDir()));

        List<HBaseSplit> splits;
        List<ConditionInfo> conditions = findConditionFromConstraint(constraint);


        // batch get
        if (Utils.isBatchGet(conditions, tableMetaInfo.getRowKeyColName())) {


            splits = getSplitsForBatchGet(conditions, tableMetaInfo, tableHandle);
            Collections.shuffle(splits);
            return new FixedSplitSource(splits);
        }
        // client side scan
        else if (isClientSideRegionScanTable(schemaName, tableName, config.getClientSideQueryModeTableNames())) {

            splits = getSplitsForClientSide(schemaName, tableName, conditions, tableMetaInfo.getRowKeyColName());
        }
        // normal scan
        else {
            splits = getSplitsForScan(conditions, tableMetaInfo);
        }

        log.info("The final split count is " + splits.size() + ".");
        splits.forEach(split -> log.info("print split info：" + split.toString()));

        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }

    /**
     * get splits for client side query mode
     *
     * @param schemaName schema name
     * @param tableName  table name
     * @param conditions conditions
     * @param rowKeyName rowKey name
     * @return splits
     */
    private List<HBaseSplit> getSplitsForClientSide(String schemaName, String tableName,
                                                    List<ConditionInfo> conditions, String rowKeyName) {
        log.info("ClientSideRegionScanner:" + schemaName + ":" + tableName);
        int hostIndex = 0;
        long createSnapshotTime = 0;
        List<HBaseSplit> splits = new ArrayList<>();
        String snapshotName = null;
        try {
            long start = System.currentTimeMillis();
            // create snapshot with retry
            snapshotName = createSnapshotWithRetry(schemaName, tableName, clientManager.getAdmin());
            createSnapshotTime = TimeTicker.calculateTimeTo(start);

            // get regions from snapshot
            List<RegionInfo> regions = Utils.getRegionInfos(config.getHbaseZookeeperQuorum(),
                    config.getZookeeperClientPort(), config.getHbaseRootDir(), snapshotName);
            // create splits
            for (RegionInfo regionInfo : regions) {
                // Client side region scanner using no startKey and endKey.
                splits.add(createHBaseSplit(schemaName, tableName, rowKeyName, hostIndex, null, null,
                        conditions, hostIndex, regionInfo, snapshotName));
                hostIndex++;
            }
        } catch (Exception e) {
            log.error(e, "E-1-1: " + e.getMessage());
        }
        log.info("create snapshot " + snapshotName + ", using " + createSnapshotTime + " mill seconds.");
        return splits;
    }

    /**
     * create snapshot with retry
     *
     * @param admin HBase admin
     * @return snapshot name
     */
    private String createSnapshotWithRetry(String schemaName, String tableName, Admin admin) {
        long start = System.currentTimeMillis();
        String snapshotName = null;
        try {
            snapshotName = "ss-" + schemaName + "." + tableName + "-" + System.nanoTime();
            HBaseMetadata.createSnapshot(snapshotName, admin, schemaName, tableName);
        } catch (Exception e) {
            log.error(e, "E-2-1: Exception: create snapshot failed, snapshotName is " + snapshotName
                    + ", track:" + e.getMessage());
            for (int i = 0; i < config.getCreateSnapshotRetryTimes(); i++) {
                try {
                    Thread.sleep(100);
                    HBaseMetadata.createSnapshot(snapshotName, admin, schemaName, tableName);
                    log.info("Recreate snapshot success! snapshotName is " + snapshotName
                            + ", retried ：" + i + " times, using " + (System.currentTimeMillis() - start) + " mill seconds.");
                    return snapshotName;
                } catch (Exception ee) {
                    log.error(ee, "E-2-2: create snapshot failed, snapshotName is " + snapshotName
                            + ", track:" + ee.getMessage());
                }
            }
            log.error("E-2-3: after retry, create snapshot " + snapshotName + " still failed.");
        }
        log.info("createSnapshotWithRetry: create snapshot " + snapshotName
                + " finished, using " + (System.currentTimeMillis() - start) + " mill seconds.");
        return snapshotName;
    }

    /**
     * check if current table using ClientSideRegionScanner to query
     *
     * @param schemaName                    schema name
     * @param tableName                     table name
     * @param clientSideQueryModeTableNames clientSideQueryModeTableNames
     * @return true or false
     */
    private boolean isClientSideRegionScanTable(String schemaName, String tableName,
                                                String clientSideQueryModeTableNames) {
        // if we didn't open client side scan, return false
        if (!config.isEnableClientSideScan()) {
            return false;
        }

        List<String> clientSideTables = getClientSideTables(clientSideQueryModeTableNames);

        Preconditions.checkState(clientSideTables != null && !clientSideTables.isEmpty(),
                "Parameter 'clientside-querymode-tablenames' cannot be NULL when 'enable-clientSide-scan' is true!" +
                        "\nSet this to * if all the table are using ClientSide query mode.");
        String schemaAndTableName = schemaName + ":" + tableName;
        return "*".equals(clientSideTables.get(0)) || clientSideTables.contains(schemaAndTableName);
    }

    private List<String> getClientSideTables(String clientSideQueryModeTableNames) {
        if (clientSideQueryModeTableNames == null) {
            return null;
        }
        return Arrays.asList(clientSideQueryModeTableNames.split(","));
    }

    /**
     * get splits for scan query mode
     *
     * @param conditions    conditions
     * @param tableMetaInfo tableMetaInfo
     * @return splits
     */
    private List<HBaseSplit> getSplitsForScan(List<ConditionInfo> conditions,
                                              TableMetaInfo tableMetaInfo) {

        String schemaName = tableMetaInfo.getSchemaName();
        String tableName = tableMetaInfo.getTableName();

        List<HBaseSplit> splits = new ArrayList<>();
        int hostIndex = 0;
        List<String> notSaltyPartStartKeyList;
        List<StartAndEnd> saltyPartStartKeyList;


        // make startKey by rowKey format and constraint.
        if (!conditions.isEmpty() && !isEmpty(tableMetaInfo.getRowKeyFormat())) {
            notSaltyPartStartKeyList = getScanStartKey(conditions, "", tableMetaInfo.getRowKeyFormat().split(","), 0);
        } else {
            notSaltyPartStartKeyList = new ArrayList<>();
        }

        // whether we can create startKeyList by constraint
        if (!notSaltyPartStartKeyList.isEmpty()) {
            // whether this table has seperate salty part at the start of rowKey
            // after version dev_0.1.5 salt value part can only have one single character
            if (config.isSeperateSaltPart()) {
                // each possible value within the range of salt value must form a finalStartKey separately with startKey
                // otherwise, duplicate data will appear in scan operation
                // therefore, the number of splits should be controlled within 100 to avoid too much performance degradation
                // so saltCount has to be Integer.MAX_VALUE
                saltyPartStartKeyList = getSaltyParts(tableMetaInfo.getRowKeyFirstCharRange(), Integer.MAX_VALUE);
                if (saltyPartStartKeyList.size() * notSaltyPartStartKeyList.size() <= MAX_SPLIT_COUNT) {
                    for (String notSaltyPartStartKey : notSaltyPartStartKeyList) {
                        for (StartAndEnd saltyPartStartKey : saltyPartStartKeyList) {
                            String finalStartKey = saltyPartStartKey.start + ROWKEY_SPLITER + notSaltyPartStartKey + ROWKEY_SPLITER;
                            splits.add(createHBaseSplit(schemaName, tableName,
                                    tableMetaInfo.getRowKeyColName(), hostIndex,
                                    finalStartKey, finalStartKey + ROWKEY_TAIL, conditions,
                                    -1, null, null));
                            hostIndex += 1;
                        }
                    }
                }
                // there are too many splits created according to the salt value + constraint
                // the performance is bad, so we have to create splits according to the salt value only
                // and do a full table scan concurrently
                else {
                    addSplitsOnlyBySaltyPart(splits, schemaName, tableName, tableMetaInfo.getRowKeyColName(),
                            conditions, tableMetaInfo.getRowKeyFirstCharRange());
                }
            }
            // there is no seperate salty part, we have to take notSaltyPartStartKeyList as startKey and stopKey
            else {
                for (String notSaltyPartStartKey : notSaltyPartStartKeyList) {
                    splits.add(createHBaseSplit(schemaName, tableName,
                            tableMetaInfo.getRowKeyColName(), hostIndex,
                            notSaltyPartStartKey + ROWKEY_SPLITER,
                            notSaltyPartStartKey + ROWKEY_SPLITER + ROWKEY_TAIL,
                            conditions, -1, null, null));
                    hostIndex += 1;
                }
            }
        } else {

            log.info("conditions-----" + conditions.toString());
            //过滤 条件中的全部的rowkey
            List<ConditionInfo> rowkeyConditions = conditions.stream()
                    .filter(key -> key.getColName().equals(tableMetaInfo.getRowKeyColName())).collect(Collectors.toList());


            //处理value值
            rowkeyConditions=rowkeyConditions.stream().peek(key -> {
                key.setValue(key.getType().getObjectValue(null, nativeValueToBlock(key.getType(), key.getValue()), 0));
            }).collect(Collectors.toList());

            log.info("rowkeyConditions--"+rowkeyConditions.toString());
            //根据value 获取start分片 和 end 分片
            ConcurrentHashMap<String, String> row = this.getRow(rowkeyConditions);



            // have no constraints to create the StartKey, and RowKey has no salt part on the prefix like '01-xxxxx',
            // have to scan full table using one single split,
            // check if the prefix of rowKey are random code so we still can create multiple splits

            if (StringUtils.isNotEmpty(tableMetaInfo.getRowKeyFirstCharRange())) {
                addSplitsOnlyBySaltyPart(splits, schemaName, tableName, tableMetaInfo.getRowKeyColName(),
                        conditions, tableMetaInfo.getRowKeyFirstCharRange());
            }
            // single split
            else {

//                // 去除 conditions 中的 rowkey相关条件
//                conditions = conditions.stream()
//                        .filter(key -> !key.getColName().equals(tableMetaInfo.getRowKeyColName())).collect(Collectors.toList());

                if (row!=null && !row.isEmpty()) {

                    splits.add(createHBaseSplit(schemaName, tableName,
                            tableMetaInfo.getRowKeyColName(), hostIndex,
                            row.get("start") + "", row.get("end") + ROWKEY_TAIL, conditions, -1, null, null));
                } else {
                    splits.add(createHBaseSplit(schemaName, tableName,
                            tableMetaInfo.getRowKeyColName(), hostIndex,
                            null, null, conditions, -1, null, null));
                }
            }

        }

        return splits;
    }

    public ConcurrentHashMap<String, String> getRow(List<ConditionInfo> rowkeyConditions) {
        ConcurrentHashMap<String, String> hashMap = new ConcurrentHashMap<String, String>();

        if(rowkeyConditions!=null && !rowkeyConditions.isEmpty()){
            rowkeyConditions.stream().forEach(key -> {
                log.info("key---"+key.toString());
                CONDITION_OPER operator = key.getOperator();
                switch (operator.name()) {
                    case "GT":
                        hashMap.put("start", String.valueOf(key.getValue()).substring(0, 1));
                        break;
                    case "LT":
                        hashMap.put("end", String.valueOf(key.getValue()).substring(0, 1));
                        break;
                    case "GE":
                        hashMap.put("start", String.valueOf(key.getValue()).substring(0, 1));
                        break;
                    case "LE":
                        hashMap.put("end", String.valueOf(key.getValue()).substring(0, 1));
                        break;
                    default:
                }
            });
            log.info("row != null");
            return hashMap;
        }else{
            log.info("row == null");
            return null;
        }
    }

    private void addSplitsOnlyBySaltyPart(List<HBaseSplit> splits, String schemaName, String tableName,
                                          String rowKeyColName, List<ConditionInfo> conditions,
                                          String rowKeyFirstCharRange) {
        log.info("Create multi-splits by the first char of rowKey, table is " + schemaName + ":" + tableName
                + ", the range of first char is : " + rowKeyFirstCharRange);
        int hostIndex = 0;


        List<StartAndEnd> startAndEndRowKeys =
                getSaltyParts(rowKeyFirstCharRange, ROWKEY_PREFIX_SPLIT_COUNT);
       if(conditions!=null&&!conditions.isEmpty()){
           //过滤 rowkey
            conditions = conditions.stream().filter(key -> key.getColName().equals(rowKeyColName)).collect(Collectors.toList());
           if(conditions!=null&&!conditions.isEmpty()){
               // 剪辑分片
               startAndEndRowKeys = this.uniteSplit(conditions, startAndEndRowKeys);
               log.info("剪辑分片后---"+startAndEndRowKeys.toString());
           }
       }

        // 去除 conditions 中的 rowkey相关条件
//        conditions = conditions.stream()
//                .filter(key -> !key.getColName().equals(rowKeyColName)).collect(Collectors.toList());


        for (StartAndEnd range : startAndEndRowKeys) {
            splits.add(createHBaseSplit(schemaName, tableName,
                    rowKeyColName, hostIndex,
                    range.start + "", range.end + ROWKEY_TAIL, conditions,
                    -1, null, null));
            hostIndex += 1;
        }
    }

    /**
     * If the first char of rowKey is hash-liked, using this to add salt to the splits.
     * will create about (0.5 ~ 1.5) * n HBase splits adjusted by constant ROWKEY_PREFIX_SPLIT_COUNT.
     *
     * @param rowKeyFirstCharRange range of the rowKey, value is like 0~9,A~F,a~f or a~f,0~9 ..
     * @return start and end rowKeys
     */
    private List<StartAndEnd> getSaltyParts(String rowKeyFirstCharRange, int saltCount) {
        List<StartAndEnd> prefixRanges = Arrays.stream(rowKeyFirstCharRange.split(COMMA))
                .map(StartAndEnd::new).collect(Collectors.toList());
        int rangeSpace = 0;
        for (StartAndEnd range : prefixRanges) {
            rangeSpace += (Math.abs(range.end - range.start) + 1);
        }

        ImmutableList.Builder<StartAndEnd> startAndEndKeys = ImmutableList.builder();
        // rounding step value
        int step = (int) Math.rint((rangeSpace + 0.0) / saltCount);
        // generate salty start and end keys
        for (StartAndEnd range : prefixRanges) {
            startAndEndKeys.addAll(generateStartEndKeyByFirstCharRangeOfRowKey(range, step));
        }

        return startAndEndKeys.build();
    }

    /**
     * @param conditions         between xxx and xxx
     * @param startAndEndRowKeys rowKeyFirstCharRange
     * @return
     */
    public List<StartAndEnd> uniteSplit(List<ConditionInfo> conditions, List<StartAndEnd> startAndEndRowKeys) {

        //获取 between的 start 和 end
        ConcurrentHashMap<String, String> betweenMap = this.getRow(conditions);

        int start = Integer.valueOf(betweenMap.get("start").charAt(0));
        int end = Integer.valueOf(betweenMap.get("end").charAt(0));

        List<StartAndEnd> uniteRowKey = new ArrayList<>();
        for (StartAndEnd startAndEnd : startAndEndRowKeys) {

            log.info("startAndEnd---"+startAndEnd.toString()+"--start--"+Integer.valueOf(startAndEnd.start)+"---end--"+Integer.valueOf(startAndEnd.end));
            log.info("start---"+start+"-----end--"+end);


            if (start < Integer.valueOf(startAndEnd.start) && end >= Integer.valueOf(startAndEnd.end)) {
                uniteRowKey.add(new StartAndEnd(startAndEnd.start,startAndEnd.end));
                break;
            }

            if (start > Integer.valueOf(startAndEnd.start) && end <= Integer.valueOf(startAndEnd.end)) {
                uniteRowKey.add(new StartAndEnd((char) start,(char) end));
                break;
            }

            if (start == Integer.valueOf(startAndEnd.start) && end<= Integer.valueOf(startAndEnd.end)) {
                uniteRowKey.add(new StartAndEnd((char) start,(char) end));
                break;
            }

            if (start == Integer.valueOf(startAndEnd.start) && end >= Integer.valueOf(startAndEnd.end)) {

                uniteRowKey.add(new StartAndEnd((char) start,startAndEnd.end));
                break;
            }


            if (end == Integer.valueOf(startAndEnd.start) && end <= Integer.valueOf(startAndEnd.end)) {

                uniteRowKey.add(new StartAndEnd(startAndEnd.start,startAndEnd.end));
                break;
            }

            if (start > Integer.valueOf(startAndEnd.start) && end >= Integer.valueOf(startAndEnd.end)) {
                uniteRowKey.add(new StartAndEnd((char) start,startAndEnd.end));
                break;
            }
        }
        return uniteRowKey;
    }

    class StartAndEnd {
        final char start;
        final char end;

        StartAndEnd(char start, char end) {
            this.start = start;
            this.end = end;
        }

        StartAndEnd(String startAndEnd) {
            String[] se = startAndEnd.split(SWUNG_DASH);
            this.start = se[0].charAt(0);
            this.end = se[1].charAt(0);
        }

        @Override
        public String toString() {
            return "StartAndEnd{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }

    /**
     * generate salty start and end keys
     *
     * @param prefixRange single prefix range
     * @param step        step
     * @return startAndEndKeys generated by one single range
     */
    private List<StartAndEnd> generateStartEndKeyByFirstCharRangeOfRowKey(StartAndEnd prefixRange, int step) {
        ImmutableList.Builder<StartAndEnd> startAndEndKeys = ImmutableList.builder();
        int realStep = step == 0 ? 1 : step;
        for (char index = prefixRange.start; index <= prefixRange.end; index += realStep) {
            char end = (char) (index + realStep - 1);
            if (index + realStep > prefixRange.end) {
                end = prefixRange.end;
            }
            startAndEndKeys.add(new StartAndEnd(index, end));
        }
        return startAndEndKeys.build();
    }

    private List<HBaseSplit> getSplitsForBatchGet(List<ConditionInfo> conditions,
                                                  TableMetaInfo tableMetaInfo,
                                                  HBaseTableHandle tableHandle) {
        log.info("BatchGet:" + tableMetaInfo.getSchemaName() + ":" + tableMetaInfo.getTableName());
        List<HBaseSplit> splits = new ArrayList<>();
        // Find all conditions of rowKey(rowKey='xxx' or rowKey in('xxx','xxx'))
        List<ConditionInfo> rowKeys = conditions.stream().filter(cond ->
                tableMetaInfo.getRowKeyColName().equals(cond.getColName())
                        && cond.getOperator() == CONDITION_OPER.EQ
        ).collect(Collectors.toList());

        int hostIndex = 0;
        int maxSplitSize;
        List<ConditionInfo> splitConditions = new ArrayList<>();
        // Each split has at least 20 pieces of data, and the maximum number of splits is 30.
        if (rowKeys.size() / BATCHGET_SPLIT_RECORD_COUNT > BATCHGET_SPLIT_MAX_COUNT) {
            maxSplitSize = rowKeys.size() / BATCHGET_SPLIT_MAX_COUNT;
        } else {
            maxSplitSize = BATCHGET_SPLIT_RECORD_COUNT;
        }
        for (ConditionInfo cond : rowKeys) {
            splitConditions.add(cond);
            // Rather than creating another split, spread the extra records into each split
            if (splitConditions.size() >=
                    maxSplitSize + (splits.size() < rowKeys.size() % BATCHGET_SPLIT_RECORD_COUNT ? 1 : 0)) {
                splits.add(createHBaseSplit(tableHandle.getSchemaTableName().getSchemaName(),
                        tableHandle.getSchemaTableName().getTableName(), tableMetaInfo.getRowKeyColName(),
                        hostIndex, null, null, splitConditions,
                        -1, null, null));
                hostIndex++;
                splitConditions = new ArrayList<>();
            } // end of if
        } // end of for
        if (splitConditions.size() > 0) {
            splits.add(createHBaseSplit(tableHandle.getSchemaTableName().getSchemaName(),
                    tableHandle.getSchemaTableName().getTableName(), tableMetaInfo.getRowKeyColName(),
                    hostIndex, null, null, splitConditions, -1, null, null));
        }
        log.info("Batch get by RowKey. Split count: "
                + splits.size() + ", table=" + tableHandle.getSchemaTableName().toString());
        for (HBaseSplit split : splits) {
            log.info("Print Split: " + split.toSimpleString());
        }
        return splits;
    }

    /**
     * create HBaseSplit for shot
     *
     * @param schemaName    schema name
     * @param tableName     table name
     * @param rowKeyColName rowkey column name
     * @param hostIndex     host index
     * @param startKey      startKey
     * @param endKey        endKey
     * @param conditions    conditions from constraint
     * @param regionIndex   region index
     * @param regionInfo    region info
     * @param snapshotName  snapshot name
     * @return HBaseSplit
     */
    private HBaseSplit createHBaseSplit(String schemaName, String tableName, String rowKeyColName, int hostIndex,
                                        String startKey, String endKey, List<ConditionInfo> conditions,
                                        int regionIndex, RegionInfo regionInfo, String snapshotName) {
        return new HBaseSplit(this.connectorId, schemaName,
                tableName, rowKeyColName, getHostAddresses(hostIndex), startKey, endKey, conditions,
                config.isRandomScheduleRedundantSplit(), regionIndex, regionInfo, snapshotName);
    }

    /**
     * get scan startKey
     *
     * @param conditions   all query conditions
     * @param tmpStartKey  tmp start key
     * @param rowKeyFormat format of rowKey
     * @param formatIndex  the index of rowKey format
     * @return A list of all startKey
     */
    private List<String> getScanStartKey(List<ConditionInfo> conditions,
                                         String tmpStartKey, String[] rowKeyFormat, int formatIndex) {
        List<String> tmpStartKeys = new ArrayList<>();
        if (formatIndex == rowKeyFormat.length) {
            tmpStartKeys.add(tmpStartKey);
            return tmpStartKeys;
        }
        List<ConditionInfo> formatCondition = conditions.stream()
                // Filter out query conditions that conform to rowKey composition and whose operator is ==
                .filter(condition -> condition.getColName().equals(rowKeyFormat[formatIndex])
                        && condition.getOperator() == CONDITION_OPER.EQ)
                .collect(Collectors.toList());
        for (ConditionInfo condition : formatCondition) {
            tmpStartKeys.addAll(
                    getScanStartKey(conditions,
                            tmpStartKey + ((isEmpty(tmpStartKey) ? "" : ",") + condition.valueToString()),
                            rowKeyFormat,
                            formatIndex + 1)
            );
        }
        return tmpStartKeys;
    }

    private List<HostAddress> getHostAddresses(int index) {
        List<HostAddress> addresses = new ArrayList<>();
        // If split-remotely-accessible is true, presto-workers-ip may be null
        if (config.isRandomScheduleRedundantSplit()) {
            String[] workers = config.getPrestoWorkersIp().split(Constant.COMMA);
            String workerIp = workers[index % workers.length];
            addresses.add(HostAddress.fromParts(workerIp, config.getPrestoServerPort()));
        }
        return addresses;
    }

    /**
     * Find all available constraint from Sql.
     * Beware, Query conditions on both sides of 'OR' cannot be obtained.
     *
     * @param constraint constraint
     * @return all available constraint from Sql
     */
    private List<ConditionInfo> findConditionFromConstraint(TupleDomain<ColumnHandle> constraint) {

        List<ConditionInfo> handles = new ArrayList<>();
        if (!constraint.getDomains().isPresent()) {
            return handles;
        }

        Map<ColumnHandle, Domain> domainMap = constraint.getDomains().orElse(null);
        Set<ColumnHandle> keySet = domainMap.keySet();

        for (ColumnHandle ch : keySet) {
            HBaseColumnHandle hch = (HBaseColumnHandle) ch;
            Domain domain = domainMap.get(hch);
            if (domain == null) {
                continue;
            }

            if (domain.isSingleValue()) {
                Object value = domain.getNullableSingleValue();
                log.info("value---" + value.toString());
                // =
                if (value instanceof Slice) {
                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.EQ,
                            value, domain.getType()));
                } else {
                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.EQ,
                            value, domain.getType()));
                }
            } else {
                log.info("-------else for");
                for (Range range : domain.getValues().getRanges().getOrderedRanges()) {

                    log.info("range -------" + range.toString());

                    if (range.isSingleValue()) {
                        log.info("if--" + range.getLowBoundedValue());
                        handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.EQ,
                                range.getSingleValue(), domain.getType()));
                    } else {
//                        Object objectValue = range.getType().getObjectValue(null, nativeValueToBlock(range.getType(), range.getLowBoundedValue()), 0);
//                        log.info("getLowBoundedValue--"+range.getLowBoundedValue());
//                        log.info("getHighBoundedValue"+range.getHighBoundedValue());
                        if (!range.isLowUnbounded()) {
                            if (range.isLowInclusive()) {
                                handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.GE,
                                        range.getLowBoundedValue(), domain.getType()));
                            } else {

                                handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.GT,
                                        range.getLowBoundedValue(), domain.getType()));
                            }

//                            switch (range.getLow().getBound()) {
//                                // >
//                                // != part 1
//                                case ABOVE:
//                                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.GT,
//                                            range.getLow().getValue(), domain.getType()));
//                                    break;
//                                // >=
//                                case EXACTLY:
//                                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.GE,
//                                            range.getLow().getValue(), domain.getType()));
//                                    break;
//                                case BELOW:
//                                    throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
//                                default:
//                                    throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
//                            }
                        }
                        if (!range.isHighUnbounded()) {
                            if (range.isHighInclusive()) {
                                handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.LT,
                                        range.getHighBoundedValue(), domain.getType()));
                            } else {
                                handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.LE,
                                        range.getHighBoundedValue(), domain.getType()));
                            }

//                            switch (range.getHigh().getBound()) {
//                                case ABOVE:
//                                    throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
//                                    // <=
//                                case EXACTLY:
//                                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.LE,
//                                            range.getHigh().getValue(), domain.getType()));
//                                    break;
//                                // <
//                                // !=
//                                case BELOW:
//                                    handles.add(new ConditionInfo(hch.getColumnName(), CONDITION_OPER.LT,
//                                            range.getHigh().getValue(), domain.getType()));
//                                    break;
//                                default:
//                                    throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
//                            }
                        }
                        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    }
                } // end of for domain.getValues().getRanges().getOrderedRanges())
            } // else of if (domain.isSingleValue)

        } // end of for domainMap.keySet

        return handles;
    }

}
