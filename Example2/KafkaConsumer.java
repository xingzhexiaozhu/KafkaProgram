package Example2;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer extends Thread{
    private ConsumerConnector consumer;
    private String topic;

    public KafkaConsumer(String topic){
        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaConf.zookeeperConnect);
        properties.put("group.id", KafkaConf.groupId);
        properties.put("zookeeper.session.timeout.ms", KafkaConf.zkSessionTimeout);
        properties.put("zookeeper.sync.time.ms", KafkaConf.zkSyncTime);
        properties.put("auto.commit.interval.ms", KafkaConf.reconnectIntervel);
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        this.topic = topic;
    }

    @Override
    public void run(){
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while(it.hasNext()){
            System.out.println("Receive: " + new String(it.next().message()));
            try{
                sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
