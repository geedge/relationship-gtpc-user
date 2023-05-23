package com.zdjizhi.common;


import com.zdjizhi.utils.system.GtpConfigurations;

/**
 * @author Administrator
 */
public  class GtpConfig {

    public static final int HBASE_SCAN_LIMIT = GtpConfigurations.getIntProperty(0, "hbase.scan.limit");


    /**
     * System
     */
    public static final String RELATION_USER_TEID_TABLE_NAME = GtpConfigurations.getStringProperty(0, "relation.user.teid.table.name");

    public static final String GTPC_KNOWLEDGE_BASE_TABLE_NAME = GtpConfigurations.getStringProperty(0, "gtpc.knowledge.base.table.name");

    /**
     * kafka
     */
    public static final String INPUT_KAFKA_SERVERS = GtpConfigurations.getStringProperty(0, "input.kafka.servers");
    public static final String HBASE_ZOOKEEPER_SERVERS = GtpConfigurations.getStringProperty(0, "hbase.zookeeper.servers");

    public static final int HBASE_RPC_TIMEOUT = GtpConfigurations.getIntProperty(0, "hbase.rpc.timeout");


    public static final String SESSION_TIMEOUT_MS = GtpConfigurations.getStringProperty(1, "session.timeout.ms");
    public static final String MAX_POLL_RECORDS = GtpConfigurations.getStringProperty(1, "max.poll.records");
    public static final String MAX_PARTITION_FETCH_BYTES = GtpConfigurations.getStringProperty(1, "max.partition.fetch.bytes");



    public static final String GROUP_ID = GtpConfigurations.getStringProperty(0, "group.id");
    public static final String INPUT_KAFKA_TOPIC = GtpConfigurations.getStringProperty(0, "input.kafka.topic");

    public static final String TOOLS_LIBRARY = GtpConfigurations.getStringProperty(0, "tools.library");
    public static final String KAFKA_USER = GtpConfigurations.getStringProperty(1, "kafka.user");
    public static final String KAFKA_PIN = GtpConfigurations.getStringProperty(1, "kafka.pin");


    public static final int CACHE_EXPIRE_SECONDS = GtpConfigurations.getIntProperty(0, "cache.expire.seconds");
    public static final int CACHE_MAX_SIZE = GtpConfigurations.getIntProperty(0, "cache.max.size");


    public static final int CACHE_UPDATE_SECONDS = GtpConfigurations.getIntProperty(0, "cache.update.seconds");

}