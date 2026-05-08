package com.weather;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class WeatherStation {

    public static void main(String[] args) throws Exception {

        String bootstrap = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092"
        );

        String topic = System.getenv().getOrDefault(
                "KAFKA_TOPIC_READINGS",
                "weather-readings"
        );

        String stationId = System.getenv().getOrDefault(
                "STATION_ID",
                "station-1"
        );

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random rand = new Random();
        long sNo = 0;

        while (true) {

            sNo++;

            if (rand.nextInt(100) < 10) {
                System.out.println("DROPPED message for station " + stationId);
                continue;
            }

            int humidity = rand.nextInt(100);
            int temp = rand.nextInt(40);
            int wind = rand.nextInt(100);

            int batteryRoll = rand.nextInt(100);

            String battery;

            if (batteryRoll < 30) {
                battery = "LOW";
            } else if (batteryRoll < 70) {
                battery = "MEDIUM";
            } else {
                battery = "HIGH";
            }

            long timestamp = Instant.now().toEpochMilli();

            String json = """
            {
              "station_id": "%s",
              "s_no": %d,
              "battery_status": "%s",
              "status_timestamp": %d,
              "weather": {
                "humidity": %d,
                "temperature": %d,
                "wind_speed": %d
              }
            }
            """.formatted(stationId, sNo, battery, timestamp, humidity, temp, wind);

            producer.send(new ProducerRecord<>(topic, stationId, json));

            System.out.println("Produced: " + json);

            Thread.sleep(1000);
        }
    }
}
