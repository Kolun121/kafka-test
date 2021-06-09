package com.example.demo.controller;


import com.example.demo.KafkaConstants;
import com.example.demo.deserializers.PersonDeserializer;
import com.example.demo.dto.PersonDTO;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Controller
@RequestMapping("/consumer")
public class ConsumerController {
    private static Consumer<Long, PersonDTO> consumer = createConsumer();

    @GetMapping
    public String getConsumerPage(Model model){

//        if(consumer == null){
//            consumer = createConsumer();
//        }

        int noMessageFound = 0;

        ArrayList<Record> records = new ArrayList<>();

        Duration duration = Duration.ofMillis(500);
        while (true) {
            System.out.println(noMessageFound);
            ConsumerRecords<Long, PersonDTO> consumerRecords = consumer.poll(duration);
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > 10)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {
                records.add(new Record(record.key(), record.value(), record.partition(), record.offset()));
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });

            consumer.commitAsync();
        }

        model.addAttribute("records", records);

        return "consumer";
    }

    private static Consumer<Long, PersonDTO> createConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "100");

        Consumer<Long, PersonDTO> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KafkaConstants.KAFKA_TOPIC));


        return consumer;
    }

    static class Record {
        public Record(Long recordKey, PersonDTO recordValue, int recordPartition, long recordOffset) {
            this.recordKey = recordKey;
            this.recordValue = recordValue;
            this.recordPartition = recordPartition;
            this.recordOffset = recordOffset;
        }

        public Long recordKey;
        public PersonDTO recordValue;
        public int recordPartition;
        public long recordOffset;
    }

}
