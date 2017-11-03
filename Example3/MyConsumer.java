package Example3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class MyConsumer {
    /**
     * 该consumer所属的组ID
     */
    private String groupid;

    /**
     * 该consumer的ID
     */
    private String consumerid;

    /**
     * 每个topic开几个线程？
     */
    private int threadPerTopic;

    public MyConsumer(String groupid, String consumerid, int threadPerTopic) {
        super();
        this.groupid = groupid;
        this.consumerid = consumerid;
        this.threadPerTopic = threadPerTopic;
    }

    public void consume() {
        Properties props = new Properties();
        props.put("group.id", groupid);
        props.put("consumer.id", consumerid);
        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("zookeeper.session.timeout.ms", "60000");
        props.put("zookeeper.sync.time.ms", "2000");
        // props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        // 设置每个topic开几个线程
        topicCountMap.put("MyTopic", threadPerTopic);

        // 获取stream
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);

        // 为每个stream启动一个线程消费消息
        for (KafkaStream<byte[], byte[]> stream : streams.get("MyTopic")) {
            new MyStreamThread(stream).start();
        }
    }

    /**
     * 每个consumer的内部线程
     */
    private class MyStreamThread extends Thread {
        private KafkaStream<byte[], byte[]> stream;
        public MyStreamThread(KafkaStream<byte[], byte[]> stream) {
            super();
            this.stream = stream;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> streamIterator = stream.iterator();

            // 逐条处理消息
            while (streamIterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> message = streamIterator.next();
                String topic = message.topic();
                int partition = message.partition();
                long offset = message.offset();
                String key = new String(message.key());
                String msg = new String(message.message());
                // 在这里处理消息,这里仅简单的输出
                // 如果消息消费失败，可以将已上信息打印到日志中，活着发送到报警短信和邮件中，以便后续处理
                System.out.println("consumerid:" + consumerid + ", thread : " + Thread.currentThread().getName()
                        + ", topic : " + topic + ", partition : " + partition + ", offset : " + offset + " , key : "
                        + key + " , mess : " + msg);
            }
        }
    }

    public static void main(String[] args) {
        String groupid = "myconsumergroup";
        MyConsumer consumer1 = new MyConsumer(groupid, "myconsumer1", 3);
        MyConsumer consumer2 = new MyConsumer(groupid, "myconsumer2", 3);
        consumer1.consume();
        consumer2.consume();
    }
}
