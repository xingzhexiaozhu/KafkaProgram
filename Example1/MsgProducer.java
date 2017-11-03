package Example1;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class MsgProducer {

    private static Producer<String, String> producer;
    private final Properties properties = new Properties();

    public MsgProducer(){
        //配置连接的Broker List
        properties.put("metadata.broker.list", "127.0.0.1:9092");
        //序列化
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        producer = new Producer<String, String>(new ProducerConfig(properties));
    }

    public static void main(String[] args){
        MsgProducer msgProducer = new MsgProducer();
        //定义topic
        String topic = "testKafka";
        //定义要发送的消息
        String msg = "2017.11.03，kafka测试";
        //构建消息对象
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
        //发送消息
        producer.send(data);
        producer.close();
    }

}
