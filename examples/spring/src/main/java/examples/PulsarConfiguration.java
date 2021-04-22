package examples;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("pulsar")
@Data
public class PulsarConfiguration {
    private String brokerServiceUrl;
    private String webServiceUrl;
    private boolean enableTransaction;
}
