package Example3;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class MyPartitioner implements Partitioner {
    public MyPartitioner(VerifiableProperties props) {

    }
    public int partition(Object key, int partitionsCount) {
        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt( stringKey.substring(offset+1)) % partitionsCount;
        }
//        partition = Integer.valueOf((String)key) % partitionsCount;
        return partition;
    }

}
