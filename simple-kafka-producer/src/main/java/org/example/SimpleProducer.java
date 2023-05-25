package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    // 토픽 이름은 Producer Record 인스턴스를 생성할 때 사용된다.
    private final static String TOPIC_NAME = "test";

    private final static String BOOTSTRAP_SERVER = "";

    public static void main(String[] args) {
        // KafkaProducer 인스턴스를 생성하기 위한 옵션들을 key/value 값으로 선언한다.
        // 필수 옵션은 반드시 선언해야 한다.
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        // 메시지 키, 메시지 값을 직렬화하기 위한 직렬화 클래스를 선언한다.
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Properties를 KafkaProducer의 생성 파라미터로 추가하여 인스턴스를 생성한다.
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessage";
        // 카프카 브로커로 데이터를 보내기 위해 ProducerRecord를 생성한다.
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
//        ProducerRecord<String, String> record = new ProducerRecord<>("test", "Pangyo", "23");
        int partitionNo = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>("test", partitionNo, "Pangyo", "23");

        // 프로듀서에서 send는 즉각적인 전송이 아니라, record를 프로듀서 내부에 가지고 있다가 배치 형태로 묶어서 브로커에 전송한다.
        producer.send(record);
        logger.info("{}", record);
        producer.flush();
        producer.close();
    }
}
