package com.example;

import com.example.avro.Movie;
import com.example.avro.RawMovie;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@SpringBootApplication
public class EventsTransformerApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventsTransformerApplication.class, args);
    }

}

@Service
@Slf4j
class MovieTransformer {

    // the topics will be automatically created if it does not exist yet
    private static final String INPUT_TOPIC = "raw-movies";
    private static final String OUTPUT_TOPIC = "movies";

    private MovieConverter movieConverter;
    private KafkaTemplate<String, Movie> kafkaTemplate;

    public MovieTransformer(MovieConverter movieConverter, KafkaTemplate<String, Movie> kafkaTemplate) {
        this.movieConverter = movieConverter;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = INPUT_TOPIC)
    public void consume(RawMovie rawMovie) {
        log.info("consumed movie:{} ", rawMovie);
        Movie movie = movieConverter.convertRawMovie(rawMovie);
        kafkaTemplate.send(OUTPUT_TOPIC, movie);
    }
}

@Service
class MovieConverter {

    public Movie convertRawMovie(RawMovie rawMovie) {

        String titleParts[] = rawMovie.getTitle().toString().split("::");
        String title = titleParts[0];
        int releaseYear = Integer.parseInt(titleParts[1]);

        return new Movie(rawMovie.getId(), title,
                releaseYear, rawMovie.getGenre());

    }

}