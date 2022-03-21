package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler{

    // invoked every time the stream has a new message

    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger log = (Logger) LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic){

        this.kafkaProducer = kafkaProducer;
        this.topic = topic;

    }

    @Override
    public void onOpen() throws Exception {
        // no need to do anything when the stream is open
    }

    @Override
    public void onClosed(){
        // when the stream closes
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        // asynchronous code
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
        // passing the message to our producer
    }

    @Override
    public void onComment(String comment) throws Exception {
        // nothing here
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in Stream Reading");
    }
}
