package cn.edu.csu.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * MinIO配置属性类，用于绑定application配置文件中的MinIO相关配置
 *
 * @author Elysia
 */
@ConfigurationProperties(prefix = "spring.minio")
public class MinioProperties {
    /**
     * MinIO服务地址
     */
    private String endpoint;


    /**
     * 访问密钥（用户名）
     */
    private String accessKey;

    /**
     * 密钥（密码）
     */
    private String secretKey;

    /**
     * 默认桶名
     */
    private String defaultBucketName = "default";

    /**
     * 是否启用HTTPS
     */
    //private boolean secure = false;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getDefaultBucketName() {
        return defaultBucketName;
    }

    public void setDefaultBucketName(String defaultBucketName) {
        this.defaultBucketName = defaultBucketName;
    }
}