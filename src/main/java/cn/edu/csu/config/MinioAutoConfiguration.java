package cn.edu.csu.config;

import cn.edu.csu.MinioFileOperateUtils;
import cn.edu.csu.properties.MinioProperties;
import io.minio.MinioClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MinIO自动配置类，用于自动配置MinIO相关的Bean
 *
 * @author Elysia
 */
@Configuration
@EnableConfigurationProperties(MinioProperties.class)
public class MinioAutoConfiguration {
    private final MinioProperties minioProperties;

    public MinioAutoConfiguration(MinioProperties minioProperties) {
        this.minioProperties = minioProperties;
    }

    /**
     * 创建MinioClient实例，根据配置决定是单节点还是集群模式
     *
     * @return MinioClient实例
     */
    @Bean
    @ConditionalOnMissingBean(MinioClient.class)
    public MinioClient minioClient() {
        return MinioClient.builder()
                .credentials(minioProperties.getAccessKey(), minioProperties.getSecretKey())
                .endpoint(minioProperties.getEndpoint())
                .build();
    }

    /**
     * 创建MinioFileOperateUtils实例，用于操作MinIO的工具类
     *
     * @param minioClient MinioClient实例
     * @return MinioFileOperateUtils实例
     */
    @Bean
    public MinioFileOperateUtils minioFileOperateUtils(MinioClient minioClient) {
        return new MinioFileOperateUtils(minioProperties, minioClient);
    }
}