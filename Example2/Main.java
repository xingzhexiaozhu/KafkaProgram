package Example2;

public class Main {
    public static void main(String[] args){
        KafkaProducer producerThread = new KafkaProducer(KafkaConf.topic1);
        producerThread.start();

        KafkaConsumer consumerThread = new KafkaConsumer(KafkaConf.topic1);
        consumerThread.start();
    }
}
