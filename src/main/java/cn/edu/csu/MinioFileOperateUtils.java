package cn.edu.csu;

import cn.edu.csu.properties.MinioProperties;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MinIO文件操作工具类，提供对MinIO服务器的各种文件操作
 *
 * @author Elysia
 */
@Component
public class MinioFileOperateUtils implements InitializingBean {
    private final MinioProperties minioProperties;
    private final MinioClient minioClient;
    private final Logger log = LoggerFactory.getLogger(MinioFileOperateUtils.class);
    /**
     * 公共访问策略模板，允许匿名用户读取桶内所有对象
     */
    private final String publicPolicy = """
            {
              "Version" : "2012-10-17",
              "Statement" : [ {
                "Effect" : "Allow",
                "Principal" : {
                  "AWS" : [ "*" ]
                },
                "Action" : [ "s3:GetObject" ],
                "Resource" : [ "arn:aws:s3:::%s/*" ]
              } ]
            }""";


    public MinioFileOperateUtils(MinioProperties minioProperties, MinioClient minioClient) {
        this.minioProperties = minioProperties;
        this.minioClient = minioClient;
    }

    /**
     * 初始化方法，在Bean初始化完成后创建默认桶
     */
    @Override
    public void afterPropertiesSet() {
        try {
            createBucket(minioProperties.getDefaultBucketName(), true);
        } catch (Exception e) {
            log.error("创建默认桶失败", e);
        }
    }

    /**
     * 创建桶，如果桶不存在则创建，并可设置为公共访问
     *
     * @param bucketName 桶名称
     * @param isPublic   是否设置为公共访问
     * @throws Exception 操作异常
     */
    public void createBucket(String bucketName, boolean isPublic) throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder()
                    .bucket(bucketName)
                    .build());
            log.info("桶{}创建成功", bucketName);
        } else {
            log.info("桶{}已存在", bucketName);
        }

        if (isPublic) {
            minioClient.setBucketPolicy(SetBucketPolicyArgs.builder()
                    .bucket(bucketName)
                    .config(publicPolicy.formatted(bucketName))
                    .build());
        }
    }

    /**
     * 上传文件到指定桶
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到MinIO后的文件名
     * @param localFilePath 本地文件路径
     * @return 公共访问URL，如果桶不是公共的则返回null
     * @throws Exception 操作异常
     */
    public String uploadFile(String bucketName, String minioFileName, String localFilePath) throws Exception {
        minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(bucketName)
                        .object(minioFileName)
                        .filename(localFilePath)
                        .build()
        );
        log.info("文件{}上传成功", minioFileName);

        String bucketPolicy = minioClient.getBucketPolicy(
                GetBucketPolicyArgs.builder()
                        .bucket(bucketName)
                        .build()
        );
        String policy = publicPolicy.formatted(bucketName);
        if (JSONObject.parseObject(policy).equals(JSONObject.parseObject(bucketPolicy))) {
            return minioProperties.getEndpoint() + "/" + bucketName + "/" + minioFileName;
        }
        return null;
    }

    /**
     * 上传文件到指定桶
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到MinIO后的文件名
     * @param file          本地文件对象
     * @return 公共访问URL，如果桶不是公共的则返回null
     * @throws Exception 操作异常
     */
    public String uploadFile(String bucketName, String minioFileName, File file) throws Exception {
        return uploadFile(bucketName, minioFileName, file.getAbsolutePath());
    }

    /**
     * 通过输入流上传文件到指定桶
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到MinIO后的文件名
     * @param inputStream   输入流
     * @param objectSize    对象大小
     * @param partSize      分片大小
     * @throws Exception 操作异常
     */
    public void putFile(String bucketName, String minioFileName, InputStream inputStream, long objectSize, long partSize) throws Exception {
        minioClient.putObject(
                PutObjectArgs.builder()
                        .stream(inputStream, objectSize, partSize)
                        .bucket(bucketName)
                        .object(minioFileName)
                        .build()
        );
        log.info("文件{}更新成功", minioFileName);
    }

    /**
     * 从指定桶下载文件
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO中的文件名
     * @param localSavePath 本地保存路径
     * @throws Exception 操作异常
     */
    public void downloadFile(String bucketName, String minioFileName, String localSavePath) throws Exception {
        minioClient.downloadObject(
                DownloadObjectArgs.builder()
                        .bucket(bucketName)
                        .object(minioFileName)
                        .filename(localSavePath)
                        .build()
        );
        log.info("文件{}下载成功", minioFileName);
    }

    /**
     * 从指定桶删除文件
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO中的文件名
     * @throws Exception 操作异常
     */
    public void deleteFile(String bucketName, String minioFileName) throws Exception {
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(bucketName)
                        .object(minioFileName)
                        .build()
        );
        log.info("文件{}删除成功", minioFileName);
    }

    /**
     * 列出指定桶中的所有文件（包括子目录）
     *
     * @param bucketName 桶名称
     * @return 文件列表
     * @throws Exception 操作异常
     */
    public List<Item> listFiles(String bucketName) throws Exception {
        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(bucketName)
                        .recursive(true)
                        .build()
        );

        List<Item> items = new ArrayList<>();
        for (Result<Item> result : results) {
            Item item = result.get();
            items.add(item);
        }
        return items;
    }

    /**
     * 获取文件的临时访问URL
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO中的文件名
     * @param expireTime    过期时间
     * @param timeUnit      时间单位
     * @return 临时访问URL
     * @throws Exception 操作异常
     */
    public String getFileTempUrl(String bucketName, String minioFileName, int expireTime, TimeUnit timeUnit) throws Exception {
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .bucket(bucketName)
                        .object(minioFileName)
                        .method(Method.GET)
                        .expiry(expireTime, timeUnit)
                        .build()
        );
    }

    /**
     * 上传文件到默认桶
     *
     * @param minioFileName 上传到MinIO后的文件名
     * @param localFilePath 本地文件路径
     * @return 公共访问URL，如果桶不是公共的则返回null
     * @throws Exception 操作异常
     */
    public String uploadFileOnDefaultBucket(String minioFileName, String localFilePath) throws Exception {
        return uploadFile(minioProperties.getDefaultBucketName(), minioFileName, localFilePath);
    }

    /**
     * 上传文件到默认桶
     *
     * @param minioFileName 上传到MinIO后的文件名
     * @param file          本地文件对象
     * @return 公共访问URL，如果桶不是公共的则返回null
     * @throws Exception 操作异常
     */
    public String uploadFileOnDefaultBucket(String minioFileName, File file) throws Exception {
        return uploadFileOnDefaultBucket(minioFileName, file.getAbsolutePath());
    }

    /**
     * 通过输入流上传文件到默认桶
     *
     * @param minioFileName 上传到MinIO后的文件名
     * @param inputStream   输入流
     * @param objectSize    对象大小
     * @param partSize      分片大小
     * @throws Exception 操作异常
     */
    public void putFileOnDefaultBucket(String minioFileName, InputStream inputStream, long objectSize, long partSize) throws Exception {
        putFile(minioProperties.getDefaultBucketName(), minioFileName, inputStream, objectSize, partSize);
    }

    /**
     * 从默认桶下载文件
     *
     * @param minioFileName MinIO中的文件名
     * @param localSavePath 本地保存路径
     * @throws Exception 操作异常
     */
    public void downloadFileOnDefaultBucket(String minioFileName, String localSavePath) throws Exception {
        downloadFile(minioProperties.getDefaultBucketName(), minioFileName, localSavePath);
    }

    /**
     * 从默认桶删除文件
     *
     * @param minioFileName MinIO中的文件名
     * @throws Exception 操作异常
     */
    public void deleteFileOnDefaultBucket(String minioFileName) throws Exception {
        deleteFile(minioProperties.getDefaultBucketName(), minioFileName);
    }

    /**
     * 列出默认桶中的所有文件（包括子目录）
     *
     * @return 文件列表
     * @throws Exception 操作异常
     */
    public List<Item> listFilesOnDefaultBucket() throws Exception {
        return listFiles(minioProperties.getDefaultBucketName());
    }

    /**
     * 获取默认桶中文件的临时访问URL
     *
     * @param minioFileName MinIO中的文件名
     * @param expireTime    过期时间
     * @param timeUnit      时间单位
     * @return 临时访问URL
     * @throws Exception 操作异常
     */
    public String getFileTempUrlOnDefaultBucket(String minioFileName, int expireTime, TimeUnit timeUnit) throws Exception {
        return getFileTempUrl(minioProperties.getDefaultBucketName(), minioFileName, expireTime, timeUnit);
    }
}