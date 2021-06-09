package com.example.demo.deserializers;

import com.example.demo.dto.PersonDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class PersonDeserializer implements Deserializer {
    @Override
    public PersonDTO deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        PersonDTO personDTO = null;
        try {
            personDTO = mapper.readValue(arg1, PersonDTO.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return personDTO;
    }

}
