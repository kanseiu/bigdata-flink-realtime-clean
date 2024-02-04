package com.kanseiu.flink.handler;

import com.kanseiu.flink.common.KafkaCommonProperties;
import com.kanseiu.flink.common.KafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 实时数据清洗 任务1
 * 使用Flink消费Kafka中topic为ods_mall_data的数据，
 * 根据数据中不同的表将数据分别分发至kafka的DWD层的fact_order_master、fact_order_detail的Topic中
 * （只获取data的内容，具体的内容格式请自查，其分区数均为2），其他的表则无需处理

 * 1、可直接运行main方法，在本地测试，注意配置kafka地址，同时取消pom中的<scope>provided</scope>
 * 2、部分flink api已经过时，不妨碍使用
 * 3、使用 mvn 打包，jar包上传到master容器内的某个路径下
 * 4、mysql -> binlog -> maxwell -> kafka-ods_mall_data -> 消费 -> 写入topic
 * 这中间没做数据转换处理，如有需要，则在读取 kafka topic 的时候再做处理
 */
public class RealTimeDataCleaningJobOne {

    private static final String READ_TOPIC = "ods_mall_data";
    private static final String MAXWELL_JSON_NODE_TABLE = "table";
    private static final String MAXWELL_JSON_NODE_DATA = "data";
    private static final String SINK_SUFFIX = "_sink";
    // 以下数组需要对位
    private static final String[] WRITE_TOPIC = {"fact_order_master", "fact_order_detail"};
    private static final String[] READ_TABLE = {"fact_order_master", "fact_order_detail"};
    private static final String[] WRITE_TABLE = {"fact_order_master", "fact_order_detail"};

    public static void main(String[] args) throws Exception {
        // 设置 Flink 环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        // Kafka 消费者配置
        Properties consumerProps = KafkaCommonProperties.getKafkaConsumerProperties(KafkaConfig.KAFKA_CONSUMER_GROUP_JOB_ONE);

        // Kafka 生产者配置
        Properties producerProps = KafkaCommonProperties.getKafkaProducerProperties();

        // 定义输出标签
        List<OutputTag<String>> outputTagList = new ArrayList<>();
        Arrays.stream(WRITE_TABLE).forEach(writeTable -> outputTagList.add(new OutputTag<String>(writeTable.concat(SINK_SUFFIX)){}));

        // 创建 Kafka 消费者
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(READ_TOPIC, new SimpleStringSchema(), consumerProps);
        DataStream<String> input = env.addSource(kafkaSource);

        // 处理逻辑
        SingleOutputStreamOperator<String> processed = input
                .process(new ProcessFunction<String, String>() {
                    private transient ObjectMapper jsonParser;
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        if (jsonParser == null) {
                            jsonParser = new ObjectMapper();
                        }
                        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                        String tableName = jsonNode.get(MAXWELL_JSON_NODE_TABLE).asText();
                        JsonNode data = jsonNode.get(MAXWELL_JSON_NODE_DATA);

                        for(int i = 0; i < READ_TABLE.length; i++) {
                            if(READ_TABLE[i].equals(tableName)) {
                                ctx.output(outputTagList.get(i), data.toString());
                            }
                        }
                    }
                });
        // 将数据发送到不同的 Kafka 主题
        for(int i = 0; i < outputTagList.size(); i++) {
            OutputTag<String> outputTag = outputTagList.get(i);
            processed.getSideOutput(outputTag).addSink(new FlinkKafkaProducer<>(WRITE_TOPIC[i], new SimpleStringSchema(), producerProps)).name(outputTag.getId());
        }

        // 执行 Flink 程序
        env.execute("RealTime Data Cleaning Job One");
    }
}