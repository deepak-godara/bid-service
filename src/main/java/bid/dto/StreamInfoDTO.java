package bid.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class StreamInfoDTO {
    private String streamKey;
    private String consumerName ;
}
