package bid.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import bid.dto.AuctionActivated;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@Configuration
public class KafkaListenerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private Map<String, Object> baseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bid-service-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 15000);
        return props;
    }

    @Bean
    public ConsumerFactory<String, AuctionActivated> activationConsumerFactory() {
        // Build ObjectMapper with JavaTimeModule so LocalDateTime fields deserialize correctly
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        JsonDeserializer<AuctionActivated> deserializer = new JsonDeserializer<>(AuctionActivated.class, mapper);
        deserializer.setUseTypeHeaders(false);
        deserializer.addTrustedPackages("*");

        Map<String, Object> props = baseConsumerProps();
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }
        @Bean
        public ProducerFactory<String, Object> retryProducerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            return new DefaultKafkaProducerFactory<>(props);
        }
    
        @Bean(name = "defaultRetryTopicKafkaTemplate")
        public KafkaTemplate<String, Object> defaultRetryTopicKafkaTemplate() {
            return new KafkaTemplate<>(retryProducerFactory());
        }
    // ─────────────────────────────────────────
    // Listener Container Factory
    // Configures manual acknowledgment mode for
    // committing offsets only after DB save succeeds
    // ─────────────────────────────────────────
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AuctionActivated> activationListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, AuctionActivated> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(activationConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    
}
