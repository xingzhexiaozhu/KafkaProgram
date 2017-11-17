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

/**
 * 参考资料：http://orchome.com/10
 *   有时，我们消费Kafka的消息，并不关心偏移量，我们仅仅关心数据能被消费就行。High Level Consumer(高级消费者)提供了消费信息的方法而屏蔽了大量的底层细节。
 *  首先要知道的是，高级消费者在zookeeper的特定分区存储最后的偏离。这个偏移当kafka启动时准备完毕。这一般是指消费者组（Consumer group）。
 *  请小心，对于kafka集群消费群体的名字是全局的，任何的“老”逻辑的消费者应该被关闭，然后运行新的代码。当一个新的进程拥有相同的消费者群的名字，kafka将会增加进程的线程消费topic并且引发的“重新平衡（reblannce）”。在这个重新平衡中，kafka将分配现有分区到所有可用线程，可能移动一个分区到另一个进程的消费分区。如果此时同时拥有旧的的新的代码逻辑，将会有一部分逻辑进入旧得Consumer而另一部分进入新的Consumer中的情况.
 *  设计一个高级消费者（Designing a High Level Consumer）
 *  了解使用高层次消费者的第一件事是，它可以（而且应该！）是一个多线程的应用。线程围绕在你的主题分区的数量，有一些非常具体的规则：
 *  如果你提供比在topic分区多的线程数量，一些线程将永远不会看到消息。
 *  如果你提供的分区比你拥有的线程多，线程将从多个分区接收数据。
 *  如果你每个线程上有多个分区，对于你以何种顺序收到消息是没有保证的。举个例子，你可能从分区10上获取5条消息和分区11上的6条消息，然后你可能一直从10上获取消息，即使11上也拥有数据。
 *  添加更多的进程线程将使kafka重新平衡，可能改变一个分区到线程的分配。
*/

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
