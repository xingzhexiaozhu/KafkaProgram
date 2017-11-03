package Example2;

public interface KafkaConf {
//    String zookeeperConnect = "10.10.196.3:2181,10.10.196.4:2181,10.10.196.5:2181";
//    String groupId = "group1";
//    String topic1 = "topic1";
//    String brokerList ="10.31.85.32:9092,10.31.85.36:9092,10.31.85.40:9092,10.31.85.44:9092,10.31.85.56:9092";
//    String zkSessionTimeout = "20000";
//    String zkSyncTime = "200";
//    String reconnectIntervel = "1000";
//
//    String topic2 = "topic2";
//    String topic3 = "topic3";

    String zookeeperConnect = "127.0.0.1:2181";
    String groupId = "group";
    String topic1 = "test20171103";
    String brokerList ="127.0.0.1:9092";
    String zkSessionTimeout = "20000";
    String zkSyncTime = "200";
    String reconnectIntervel = "1000";
}
