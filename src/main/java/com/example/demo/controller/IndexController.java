package com.example.demo.controller;


import com.example.demo.KafkaConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Controller
public class IndexController {
    @GetMapping
    public String getProducerPage(){
        return "index";
    }
}
