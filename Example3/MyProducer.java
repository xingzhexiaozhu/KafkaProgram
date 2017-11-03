package Example3;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

public class MyProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "Example3.MyPartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        //单条发送
        for (long n = 0; n < 1000000; n++) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("MyTopic", n+"", "Message_"+n);
            producer.send(data);
            Thread.sleep(300);
        }

        //批量发送
        List<KeyedMessage<String, String>> messageList = new ArrayList<KeyedMessage<String, String>>(100);
        for (int i=0; i<=10000; i++){
            KeyedMessage<String, String> message = new KeyedMessage<String, String>("MyTopic", i+"", "Message_"+i);
            messageList.add(message);
            if (i % 100 == 0){
                producer.send(messageList);
                messageList.clear();
            }
        }
        producer.send(messageList);
    }
}
