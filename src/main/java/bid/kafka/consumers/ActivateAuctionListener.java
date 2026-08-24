package bid.kafka.consumers;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;


import bid.dto.AuctionActivated;
import bid.model.Auction;
import bid.repository.AuctionRepository;
import lombok.RequiredArgsConstructor;
// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class ActivateAuctionListener {

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final AuctionRepository auctionRepository;

    @RetryableTopic(
        attempts = "3",
        backOff = @BackOff(delay = 1000, multiplier = 2.0),
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltTopicSuffix = ".DLT"
    )
    @KafkaListener(
        topics = "activate-auction",
        groupId = "bid-service-group",
        containerFactory = "activationListenerContainerFactory",
        concurrency = "3"
    )
    public void listenActivateAuctionEvent(
            AuctionActivated event,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        try {
            // 1. DB write — source of truth
            String biddersJson = objectMapper.writeValueAsString(event.getRegisteredUsers());
            Auction auction = Auction.builder()
                    .auctionId(event.getId())
                    .startTime(event.getStartTime())
                    .endTime(event.getEndTime())
                    .basePrice(event.getBasePrice())
                    .minBidIncrement(event.getMinBidIncrement())
                    .registeredBidderIds(biddersJson)
                    .build();
            auctionRepository.save(auction);

            // 2. Redis meta — individual fields, single round trip
            String auctionKey = "auction:{" + auction.getAuctionId() + "}:meta";

            Map<String, String> fields = new HashMap<>();
            fields.put("startTime",       event.getStartTime().toString());
            fields.put("endTime",         event.getEndTime().toString());
            fields.put("basePrice",       event.getBasePrice().toString());
            fields.put("minBidIncrement", event.getMinBidIncrement().toString());
            redisTemplate.opsForHash().putAll(auctionKey, fields);

            // 3. Registered bidders — Set for O(1) membership check
            String registeredKey = "auction:{" + auction.getAuctionId() + "}:registered";
            redisTemplate.opsForSet().add(registeredKey, event.getRegisteredUsers().toArray(new String[0]));
            ack.acknowledge();

        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize auction event", e);
        }
    }
}
