package com.kanseiu.flink.handler;

import com.alibaba.fastjson.JSONObject;
import com.kanseiu.flink.common.KafkaCommonProperties;
import com.kanseiu.flink.common.KafkaConfig;
import org.apache.commons.lang3.StringUtils;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

/**
 * 实时数据清洗 任务2
 * 使用Flink消费Kafka中topic为ods_mall_log的数据，
 * 根据数据中不同的表前缀区分，过滤出product_browse的数据，
 * 将数据分别分发至kafka的DWD层log_product_browse的Topic中，其分区数为2，其他的表则无需处理

 * 注：
 * 1、可直接运行main方法，在本地测试，注意配置kafka地址，同时取消pom中的<scope>provided</scope>
 * 2、部分flink api已经过时，不妨碍使用
 * 3、使用 mvn 打包，jar包上传到master容器内的某个路径下
 * 4、log_id的字段类型我改成了String，这样可以符合"可以使用随机数（0-9）+MMddHHmmssSSS代替"的要求
 */
public class RealTimeDataCleaningJobTwo {
    private static final String READ_TOPIC = "ods_mall_log";
    private static final String SINK_SUFFIX = "_sink";
    private static final String WRITE_TOPIC = "log_product_browse";
    private static final String READ_TABLE = "product_browse";
    private static final String WRITE_TABLE = "log_product_browse";
    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("MMddHHmmssSSS");
    private static final DateTimeFormatter DTF2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public static void main(String[] args) throws Exception {
        // 设置 Flink 环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        // Kafka 消费者配置
        Properties consumerProps = KafkaCommonProperties.getKafkaConsumerProperties(KafkaConfig.KAFKA_CONSUMER_GROUP_JOB_TWO);

        // Kafka 生产者配置
        Properties producerProps = KafkaCommonProperties.getKafkaProducerProperties();

        // 定义输出标签
        OutputTag<String> outputTag = new OutputTag<String>(WRITE_TABLE.concat(SINK_SUFFIX)){};

        // 创建 Kafka 消费者
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(READ_TOPIC, new SimpleStringSchema(), consumerProps);
        DataStream<String> input = env.addSource(kafkaSource);

        // 处理逻辑
        SingleOutputStreamOperator<String> processed = input
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) {
                        // 处理数据
                        if(StringUtils.isNotEmpty(value) && value.startsWith(READ_TABLE)) {
                            String rawData = value.substring("product_browse:".length()).replaceAll("[()]", "");
                            // 拆分数据
                            String[] parts = rawData.split("\\|");

                            // 创建 JSON 对象
                            JSONObject json = new JSONObject();

                            Random random = new Random();
                            int randomNumber = random.nextInt(10);
                            String logId = randomNumber + "+" + LocalDateTime.now().format(DTF);

                            json.put("log_id", logId);
                            json.put("product_id", parts[0]);
                            json.put("customer_id", Integer.parseInt(parts[1]));
                            json.put("gen_order", Integer.parseInt(parts[2]));
                            json.put("order_sn", parts[3].replaceAll("'", ""));
                            json.put("modified_time", LocalDateTime.parse(parts[4].replaceAll("'", ""), DTF2));

                            String logProductBrowseRecord = json.toString();
                            ctx.output(outputTag, logProductBrowseRecord);
                        }
                    }
                });
        // 将数据发送到 Kafka 主题
        processed.getSideOutput(outputTag).addSink(new FlinkKafkaProducer<>(WRITE_TOPIC, new SimpleStringSchema(), producerProps)).name(outputTag.getId());

        // 执行 Flink 程序
        env.execute("RealTime Data Cleaning Job Two");
    }
}