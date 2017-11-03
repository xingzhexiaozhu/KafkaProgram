package Example2;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducer extends Thread{
    private Producer<Integer, String> producer;
    private String topic;

    public KafkaProducer(String topic){
        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", KafkaConf.brokerList);
        producer = new Producer<>(new ProducerConfig(properties));
        this.topic = topic;
    }

    @Override
    public void run(){
        int messageNo = 1;
        while(true){
            String message = new String("Message_" + messageNo);
            System.out.println("Send: " + message);
            producer.send(new KeyedMessage<>(topic, message));
            messageNo++;
            try{
                sleep(3000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
