package Example1;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MsgConsumer {

    private ConsumerConnector consumer;
    private String topic;

    public MsgConsumer(String zookeeper, String groupId, String topic){
        Properties properties = new Properties();
        //配置zookeeper信息
        properties.put("zookeeper.connect", "127.0.0.1:2181");
        //配置消费群组
        properties.put("group.id", "Msg");
        //配置超时连接
        properties.put("zookeeper.session.timeout.ms", "500");
        //配置重连接时间间隔
        properties.put("auto.commit.interval.ms", "1000");
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        this.topic = topic;
    }

    public void testConsumer(){
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        //定义订阅topic数量
        topicCount.put(topic, new Integer(1));
        //返回的是所有topic的Map
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        //取出我们需要的topic中的消息流
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
            while (consumerIte.hasNext()) {
                System.out.println(new String(consumerIte.next().message()));
            }
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public static void main(String[] args){
        String topic = "testKafka";
        MsgConsumer msgConsumer = new MsgConsumer("127.0.0.1:2181", "Msg", topic);
        msgConsumer.testConsumer();
    }

}
