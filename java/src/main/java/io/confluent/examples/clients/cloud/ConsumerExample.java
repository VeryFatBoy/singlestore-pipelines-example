/**
 * Copyright 2020 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.clients.cloud;

import org.apache.kafka.clients.consumer.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerExample {

    private static final String HOSTNAME = "<host>";
    private static final String PORT = "3306";
    private static final String USER = "admin";
    private static final String PASSWORD = "<password>";
    private static final String DATABASE = "sensor_readings";

    public static void main(final String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Please provide command line arguments: configPath topic");
            System.exit(1);
        }

        // Load properties from a local configuration file
        // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
        // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
        // Follow these instructions to create this file: https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java.html

        final Properties props = loadConfig(args[0]);

        final String topic = args[1];

        // Add additional properties.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            verifyDriver();

            String connection = String.format("jdbc:mysql://%s:%s/%s", HOSTNAME, PORT, DATABASE);
            Connection conn = DriverManager.getConnection(connection, USER, PASSWORD);

            PreparedStatement stmt = conn.prepareStatement("INSERT IGNORE INTO temperatures (sensorid, temp, ts) VALUES (?, ?, ?)");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {

                    int sensorId = Integer.parseInt(record.key());
                    String[] value = record.value().split(",");
                    double temp = Double.parseDouble(value[1]);
                    long ts = Long.parseLong(value[2]);

                    stmt.setInt(1, sensorId);
                    stmt.setDouble(2, temp);
                    stmt.setLong(3, ts);
                    stmt.executeUpdate();
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static Properties loadConfig(String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public static void verifyDriver() throws ClassNotFoundException {
        Class.forName("org.mariadb.jdbc.Driver");
    }
}
