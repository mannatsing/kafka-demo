package com.example;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class ProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}

}

@RestController
@Slf4j
class Producer {

	private static final String TOPIC = "greetings";
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

//	public Producer(KafkaTemplate<String, String> kafkaTemplate) {
//		this.kafkaTemplate = kafkaTemplate;
//	}

	@PostMapping("/messages")
	public void sendMessage(@RequestBody String message) {
		log.info(String.format("#### -> Producing message -> %s", message));
		this.kafkaTemplate.send(TOPIC, message);
	}
}
