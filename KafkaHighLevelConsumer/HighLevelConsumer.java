package KafkaHighLevelConsumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ConsumerTest implements Runnable{
    private KafkaStream stream;
    private int threadNum;

    public ConsumerTest(KafkaStream stream, int threadNum){
        this.stream = stream;
        this.threadNum = threadNum;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while(it.hasNext()){
            System.out.println("Thread " + threadNum + ": " + new String(it.next().message()));
        }
        System.out.println("Shutting down thread: " + threadNum);
    }
};

public class HighLevelConsumer {
    private ConsumerConnector consumer;
    private String topic;
    private ExecutorService executor;

    public HighLevelConsumer(String zookeeper, String groupId, String topic){
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        this.topic = topic;
    }

    private ConsumerConfig createConsumerConfig(String zookeeper, String groupId){
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeeper);
        properties.put("group.id", groupId);
        properties.put("zookeeper.session.timeout.ms", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(properties);
    }

    public void shutdown(){
        if(consumer != null)
            consumer.shutdown();
        if(executor != null)
            executor.shutdown();
    }

    public void run(int threadNum){
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(threadNum));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(threadNum);
        int threadIndex = 0;
        for( KafkaStream stream : streams){
            executor.submit(new ConsumerTest(stream, threadIndex));
            threadIndex++;
        }
    }

    public static void main(String[] args){
        String zookeeper = "127.0.0.1:2181";
        String groupId = "groupId";
        String topic = "topicName";
        int threads = 5;

        HighLevelConsumer consumer = new HighLevelConsumer(zookeeper, groupId, topic);
        consumer.run(threads);
        try {
            Thread.sleep(1000);
        }catch (InterruptedException e){
            System.out.println(e);
        }
        consumer.shutdown();
    }
}
