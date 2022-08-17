package com.zdjizhi.utils.kafka;

import com.zdjizhi.common.GtpConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.kafka
 * @Description:
 * @date 2021/9/610:37
 */
class CertUtils {
    /**
     * Kafka SASL认证端口
     */
    private static final String SASL_PORT = "9094";

    /**
     * Kafka SSL认证端口
     */
    private static final String SSL_PORT = "9095";

    /**
     * 根据连接信息端口判断认证方式。
     *
     * @param servers    kafka 连接信息
     * @param properties kafka 连接配置信息
     */
    static void chooseCert(String servers, Properties properties) {
        if (servers.contains(SASL_PORT)) {
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.mechanism", "PLAIN");
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                    + GtpConfig.KAFKA_USER + " password=" + GtpConfig.KAFKA_PIN + ";");
        } else if (servers.contains(SSL_PORT)) {
            properties.put("security.protocol", "SSL");
            properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            properties.put("ssl.keystore.location", GtpConfig.TOOLS_LIBRARY + "keystore.jks");
            properties.put("ssl.keystore.password", GtpConfig.KAFKA_PIN);
            properties.put("ssl.truststore.location", GtpConfig.TOOLS_LIBRARY + "truststore.jks");
            properties.put("ssl.truststore.password", GtpConfig.KAFKA_PIN);
            properties.put("ssl.key.password", GtpConfig.KAFKA_PIN);
        }

    }
}
