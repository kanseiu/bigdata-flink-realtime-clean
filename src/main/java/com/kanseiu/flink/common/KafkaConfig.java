package com.kanseiu.flink.common;

public class KafkaConfig {

    public final static String KAFKA_BOOTSTRAP_SERVERS = "master:9092";

    public final static String KAFKA_CONSUMER_GROUP_JOB_ONE = "realtime-data-cleaning-job-one-group";

    public final static String KAFKA_CONSUMER_GROUP_JOB_TWO = "realtime-data-cleaning-job-two-group";

    public final static String CONSUMER_OFFSET = "latest";
}
