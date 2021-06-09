package com.example.demo.controller;


import com.example.demo.KafkaConstants;
import com.example.demo.dto.PersonDTO;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Controller
@RequestMapping("/producer")
public class ProducerController {
    private static int producer_record_index = 0;
    private static Producer<Long, PersonDTO> producer = createProducer();

    @GetMapping
    public String getProducerPage(Model model){
        model.addAttribute("person", new PersonDTO());

        return "producer";
    }

    @PostMapping
    public String postRecord(PersonDTO personDTO){

        System.out.println(personDTO.getAge());
        System.out.println(personDTO.getName());
        System.out.println(personDTO.getSurname());

        ProducerRecord<Long, PersonDTO> record = new ProducerRecord<Long, PersonDTO>(KafkaConstants.KAFKA_TOPIC,
                Long.valueOf(producer_record_index++), personDTO);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Record sent with key " + producer_record_index + " to partition " + metadata.partition()
                    + " with offset " + metadata.offset());
        }
        catch (ExecutionException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
        catch (InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }

        return "redirect:/producer";
    }

//    public static Producer<Long, PersonDTO> createProducer() {
//        Properties props = new Properties();
//
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKER);
//        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstants.CLIENT_ID);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
//        return new KafkaProducer<>(props);
//    }
    public static Producer<Long, PersonDTO> createProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
