package com.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.*;

public class CentralStation {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String readingsTopic = System.getenv().getOrDefault(
                "KAFKA_TOPIC_READINGS",
                "weather-readings"
        );
        
        String alertsTopic = System.getenv().getOrDefault(
                        "KAFKA_TOPIC_ALERTS",
                        "rain-alerts"
                );

        String dbHost = System.getenv().getOrDefault("DB_HOST", "localhost");
        String dbPort = System.getenv().getOrDefault("DB_PORT", "5432");
        String dbName = System.getenv().getOrDefault("DB_NAME", "weatherdb");
        String dbUser = System.getenv().get("DB_USER");
        String dbPass = System.getenv("DB_PASSWORD");
        if (dbPass == null || dbPass.isEmpty()) {
          dbPass = "weatherpass";
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(
        readingsTopic,
        alertsTopic
));

        String url = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;

        try (Connection conn = DriverManager.getConnection(url, dbUser, dbPass)) {

            String sql = """
                INSERT INTO weather_readings
                (station_id, s_no, battery_status, status_timestamp, humidity, temperature, wind_speed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

            PreparedStatement stmt = conn.prepareStatement(sql);

            List<String> batch = new ArrayList<>();
            int count = 0;

            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                if (record.topic().equals(alertsTopic)) {

                    JsonNode alertNode = mapper.readTree(record.value());

                    System.out.println(
                            "RAIN ALERT -> Station: "
                            + alertNode.get("station_id").asText()
                            + " | Humidity: "
                            + alertNode.get("weather").get("humidity").asInt()
                    );

                    continue;
                }

                    batch.add(record.value());
                    count++;

                    if (count >= 100) { // safer than 5000 for K8s demo

                        for (String msg : batch) {

                            JsonNode node = mapper.readTree(msg);
                            JsonNode weather = node.get("weather");

                            stmt.setString(1, node.get("station_id").asText());
                            stmt.setLong(2, node.get("s_no").asLong());
                            stmt.setString(3, node.get("battery_status").asText());
                            stmt.setLong(4, node.get("status_timestamp").asLong());
                            stmt.setInt(5, weather.get("humidity").asInt());
                            stmt.setInt(6, weather.get("temperature").asInt());
                            stmt.setInt(7, weather.get("wind_speed").asInt());

                            stmt.addBatch();
                        }

                        stmt.executeBatch();

                        System.out.println("Inserted batch of " + count);

                        batch.clear();
                        count = 0;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
