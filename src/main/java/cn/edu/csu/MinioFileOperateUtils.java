package cn.edu.csu;

import cn.edu.csu.properties.MinioProperties;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * MinIO 文件操作工具类
 * <p>
 * 提供对 MinIO 服务器的各种基础及进阶文件操作，包括文件上传、下载、删除、批量处理以及临时链接生成等。<br>
 * 本工具类已将底层框架抛出的所有受检异常（Checked Exception）统一捕获并包装为 {@link RuntimeException} 抛出，
 * 调用方可根据业务需要选择性捕获，避免代码中出现大量的 try-catch 块。
 * </p>
 *
 * @author Elysia
 */
@Component
public class MinioFileOperateUtils implements InitializingBean {
    private final MinioProperties minioProperties;
    private final MinioClient minioClient;
    private final Logger log = LoggerFactory.getLogger(MinioFileOperateUtils.class);

    /**
     * 公共访问策略模板，允许匿名用户读取桶内所有对象（s3:GetObject）
     */
    private final String publicPolicy = """
            {
              "Version" : "2012-10-17",
              "Statement" :[ {
                "Effect" : "Allow",
                "Principal" : {
                  "AWS" : [ "*" ]
                },
                "Action" : [ "s3:GetObject" ],
                "Resource" :[ "arn:aws:s3:::%s/*" ]
              } ]
            }""";

    /**
     * 未知流大小上传时的默认分片大小 (10MB)
     */
    private static final long DEFAULT_PART_SIZE = 10 * 1024 * 1024;

    /**
     * 构造函数注入 MinIO 属性配置和客户端实例
     *
     * @param minioProperties MinIO 配置信息
     * @param minioClient     MinIO 客户端
     */
    public MinioFileOperateUtils(MinioProperties minioProperties, MinioClient minioClient) {
        this.minioProperties = minioProperties;
        this.minioClient = minioClient;
    }

    /**
     * Spring Bean 初始化完成后执行的方法。<br>
     * 用于在项目启动时检查并自动创建默认存储桶，如果连接失败会阻断启动，以便尽早发现配置错误。
     *
     * @throws RuntimeException 当 MinIO 连接失败或初始化异常时抛出
     */
    @Override
    public void afterPropertiesSet() {
        try {
            createBucket(minioProperties.getDefaultBucketName(), true);
            log.info("MinIO 默认桶 [{}] 初始化检查完成", minioProperties.getDefaultBucketName());
        } catch (Exception e) {
            log.error("初始化检查 MinIO 默认桶失败，请检查配置或服务器状态！", e);
            throw new RuntimeException("MinIO 初始化失败", e);
        }
    }

    /**
     * 内部通用方法：拼接并构建文件的公共访问 URL
     *
     * @param bucketName    桶名称
     * @param minioFileName 文件在 MinIO 中的对象名称
     * @return 完整的可访问 URL 字符串
     */
    private String buildPublicUrl(String bucketName, String minioFileName) {
        String endpoint = minioProperties.getEndpoint();
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        return endpoint + "/" + bucketName + "/" + minioFileName;
    }

    //  桶操作 

    /**
     * 创建桶。如果桶不存在则创建，并可根据参数设置为允许匿名访问（公共读）。
     *
     * @param bucketName 桶名称
     * @param isPublic   是否设置为公共访问 (true: 公共读, false: 私有)
     * @throws RuntimeException 操作失败时抛出
     */
    public void createBucket(String bucketName, boolean isPublic) {
        try {
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                log.info("桶 {} 创建成功", bucketName);
            }

            if (isPublic) {
                minioClient.setBucketPolicy(SetBucketPolicyArgs.builder()
                        .bucket(bucketName)
                        .config(publicPolicy.formatted(bucketName))
                        .build());
            }
        } catch (Exception e) {
            log.error("创建或配置桶 {} 失败", bucketName, e);
            throw new RuntimeException("创建桶失败", e);
        }
    }

    //  文件上传 

    /**
     * 根据本地文件路径，上传文件到指定桶
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param localFilePath 本地文件的绝对路径
     * @return 文件的公共访问 URL
     * @throws RuntimeException 上传失败时抛出
     */
    public String uploadFile(String bucketName, String minioFileName, String localFilePath) {
        try {
            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(minioFileName)
                            .filename(localFilePath)
                            .build()
            );
            log.info("本地文件 {} 上传成功", minioFileName);
            return buildPublicUrl(bucketName, minioFileName);
        } catch (Exception e) {
            log.error("本地文件 {} 上传失败", minioFileName, e);
            throw new RuntimeException("文件上传失败", e);
        }
    }

    /**
     * 根据本地 File 对象，上传文件到指定桶
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param file          本地 File 对象
     * @return 文件的公共访问 URL
     * @throws RuntimeException 上传失败时抛出
     */
    public String uploadFile(String bucketName, String minioFileName, File file) {
        return uploadFile(bucketName, minioFileName, file.getAbsolutePath());
    }

    /**
     * 基础流式上传（推荐 Web 接口处理 MultipartFile 时使用此方法）
     * <p>
     * 支持传入精确大小和类型，若文件大于 5MB，内部会自动启用分片上传。
     * <b>注意：调用方需负责关闭传入的 InputStream 资源。</b>
     * </p>
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param inputStream   数据输入流
     * @param objectSize    对象的精确大小（字节），未知可传 -1
     * @param contentType   文件的 MIME 类型（如 "image/jpeg"），为空则由 MinIO 自动推断
     * @return 文件的公共访问 URL
     * @throws RuntimeException 上传失败时抛出
     */
    public String putFile(String bucketName, String minioFileName, InputStream inputStream, long objectSize, String contentType) {
        try {
            // 如果文件已知大小且小于 5MB，分片大小传 -1；否则强制开启分片上传
            long partSize = (objectSize > -1 && objectSize <= 5 * 1024 * 1024) ? -1 : DEFAULT_PART_SIZE;

            minioClient.putObject(
                    PutObjectArgs.builder()
                            .stream(inputStream, objectSize, partSize)
                            .bucket(bucketName)
                            .object(minioFileName)
                            .contentType(contentType)
                            .build()
            );
            log.info("流式文件 {} 上传成功", minioFileName);
            return buildPublicUrl(bucketName, minioFileName);
        } catch (Exception e) {
            log.error("流式文件 {} 上传失败", minioFileName, e);
            throw new RuntimeException("流式文件上传失败", e);
        }
    }

    /**
     * 重载流式上传：适用于完全未知大小、无特定格式要求的流式数据。<br>
     * 内部强制使用 10MB 分片大小进行分片上传。
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param inputStream   数据输入流
     * @return 文件的公共访问 URL
     * @throws RuntimeException 上传失败时抛出
     */
    public String putFile(String bucketName, String minioFileName, InputStream inputStream) {
        return putFile(bucketName, minioFileName, inputStream, -1, null);
    }

    /**
     * 批量上传文件到指定桶。<br>
     * 底层采用 Java 并行流 (parallelStream) 和 ConcurrentHashMap 实现高并发上传，大幅提升批量处理效率。<br>
     * 单个文件上传失败不会阻断其他文件的上传，但会在日志中记录错误，最终返回的是成功上传的文件集合。
     *
     * @param bucketName 目标桶名称
     * @param nameToPath Map 集合，Key 为 MinIO 文件名，Value 为本地文件绝对路径
     * @return 成功上传的文件集合，Key 为文件名，Value 为公共访问 URL
     */
    public Map<String, String> batchUploadFile(String bucketName, Map<String, String> nameToPath) {
        Map<String, String> nameToUrl = new ConcurrentHashMap<>();

        nameToPath.entrySet().parallelStream().forEach(entry -> {
            String minioFileName = entry.getKey();
            String localFilePath = entry.getValue();
            try {
                String url = uploadFile(bucketName, minioFileName, localFilePath);
                nameToUrl.put(minioFileName, url);
            } catch (Exception e) {
                // 单个文件失败不阻断其他文件上传，但记录错误日志
                log.error("文件 {} 批量上传失败", minioFileName, e);
            }
        });
        return nameToUrl;
    }

    //  文件下载与读取 

    /**
     * 从指定桶将文件下载至服务器本地磁盘
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @param localSavePath 本地保存的绝对路径（需包含文件名）
     * @throws RuntimeException 下载失败时抛出
     */
    public void downloadFile(String bucketName, String minioFileName, String localSavePath) {
        try {
            minioClient.downloadObject(
                    DownloadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(minioFileName)
                            .filename(localSavePath)
                            .build()
            );
            log.info("文件 {} 成功下载至本地 {}", minioFileName, localSavePath);
        } catch (Exception e) {
            log.error("文件 {} 下载失败", minioFileName, e);
            throw new RuntimeException("文件下载失败", e);
        }
    }

    /**
     * 获取文件的数据输入流（适用于后端直接向前端返回文件字节流，实现零落盘下载）。<br>
     * <b>极其重要：调用方读取完毕后，必须手动调用 {@code close()} 方法关闭输入流，否则将导致连接和内存泄漏！</b>
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @return 文件的输入流
     * @throws RuntimeException 获取流失败或文件不存在时抛出
     */
    public InputStream getFileStream(String bucketName, String minioFileName) {
        try {
            return minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(minioFileName)
                            .build()
            );
        } catch (Exception e) {
            log.error("获取文件 {} 的数据流失败", minioFileName, e);
            throw new RuntimeException("获取文件流失败", e);
        }
    }

    //  文件信息与管理 

    /**
     * 从指定桶中删除文件
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @throws RuntimeException 删除失败时抛出
     */
    public void deleteFile(String bucketName, String minioFileName) {
        try {
            minioClient.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(bucketName)
                            .object(minioFileName)
                            .build()
            );
            log.info("文件 {} 删除成功", minioFileName);
        } catch (Exception e) {
            log.error("文件 {} 删除失败", minioFileName, e);
            throw new RuntimeException("删除文件失败", e);
        }
    }

    /**
     * 获取文件的元数据信息（可用于判断文件是否存在、获取文件大小、ContentType 等）
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @return {@link StatObjectResponse} 包含文件元数据的响应对象
     * @throws RuntimeException 获取失败或文件不存在时抛出
     */
    public StatObjectResponse getFileInfo(String bucketName, String minioFileName) {
        try {
            return minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucketName)
                            .object(minioFileName)
                            .build()
            );
        } catch (Exception e) {
            log.error("获取文件 {} 属性失败，可能文件不存在", minioFileName, e);
            throw new RuntimeException("获取文件属性失败", e);
        }
    }

    /**
     * 递归列出指定桶中的所有文件（包括子目录下的文件）
     *
     * @param bucketName 桶名称
     * @return 包含文件信息的 {@link Item} 列表
     * @throws RuntimeException 查询失败时抛出
     */
    public List<Item> listFiles(String bucketName) {
        try {
            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucketName)
                            .recursive(true)
                            .build()
            );

            List<Item> items = new ArrayList<>();
            for (Result<Item> result : results) {
                items.add(result.get());
            }
            return items;
        } catch (Exception e) {
            log.error("列出桶 {} 内的文件失败", bucketName, e);
            throw new RuntimeException("获取文件列表失败", e);
        }
    }

    /**
     * 生成文件的临时（预签名）访问 URL，适用于私有桶的文件分享与下载。
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @param expireTime    过期时间数值
     * @param timeUnit      时间单位（如 TimeUnit.HOURS）
     * @return 带有预签名的临时访问 URL 字符串
     * @throws RuntimeException 生成失败时抛出
     */
    public String getFileTempUrl(String bucketName, String minioFileName, int expireTime, TimeUnit timeUnit) {
        try {
            return minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .bucket(bucketName)
                            .object(minioFileName)
                            .method(Method.GET)
                            .expiry(expireTime, timeUnit)
                            .build()
            );
        } catch (Exception e) {
            log.error("获取文件 {} 的临时URL失败", minioFileName, e);
            throw new RuntimeException("获取临时URL失败", e);
        }
    }

    /**
     * 仅拼接并获取已有文件的公共访问 URL，不涉及任何底层上传或验证操作。
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @return 公共访问 URL
     */
    public String getFileUrl(String bucketName, String minioFileName) {
        return buildPublicUrl(bucketName, minioFileName);
    }

    //  默认桶快捷操作 
    // 以下方法均是对上方基础方法的封装，固定操作 application.yml 中配置的 defaultBucketName

    /**
     * 上传本地文件到默认桶
     *
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param localFilePath 本地文件的绝对路径
     * @return 公共访问 URL
     */
    public String uploadFileOnDefaultBucket(String minioFileName, String localFilePath) {
        return uploadFile(minioProperties.getDefaultBucketName(), minioFileName, localFilePath);
    }

    /**
     * 上传本地 File 对象到默认桶
     *
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param file          本地 File 对象
     * @return 公共访问 URL
     */
    public String uploadFileOnDefaultBucket(String minioFileName, File file) {
        return uploadFile(minioProperties.getDefaultBucketName(), minioFileName, file);
    }

    /**
     * 将输入流按指定大小和类型上传到默认桶
     *
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param inputStream   数据输入流
     * @param objectSize    精确字节大小，未知传 -1
     * @param contentType   文件 MIME 类型
     * @return 公共访问 URL
     */
    public String putFileOnDefaultBucket(String minioFileName, InputStream inputStream, long objectSize, String contentType) {
        return putFile(minioProperties.getDefaultBucketName(), minioFileName, inputStream, objectSize, contentType);
    }

    /**
     * 将未知大小的输入流上传到默认桶
     *
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param inputStream   数据输入流
     * @return 公共访问 URL
     */
    public String putFileOnDefaultBucket(String minioFileName, InputStream inputStream) {
        return putFile(minioProperties.getDefaultBucketName(), minioFileName, inputStream);
    }

    /**
     * 批量上传文件到默认桶
     *
     * @param nameToPath Map 集合，Key 为 MinIO 文件名，Value 为本地文件绝对路径
     * @return 成功上传的文件集合，Key 为文件名，Value 为公共访问 URL
     */
    public Map<String, String> batchUploadFileOnDefaultBucket(Map<String, String> nameToPath) {
        return batchUploadFile(minioProperties.getDefaultBucketName(), nameToPath);
    }

    /**
     * 从默认桶下载文件到本地路径
     *
     * @param minioFileName MinIO 中的对象文件名
     * @param localSavePath 本地保存的绝对路径
     */
    public void downloadFileOnDefaultBucket(String minioFileName, String localSavePath) {
        downloadFile(minioProperties.getDefaultBucketName(), minioFileName, localSavePath);
    }

    /**
     * 获取默认桶中文件的输入流（需手动关闭流）
     *
     * @param minioFileName MinIO 中的对象文件名
     * @return 文件的输入流
     */
    public InputStream getFileStreamOnDefaultBucket(String minioFileName) {
        return getFileStream(minioProperties.getDefaultBucketName(), minioFileName);
    }

    /**
     * 从默认桶删除文件
     *
     * @param minioFileName MinIO 中的对象文件名
     */
    public void deleteFileOnDefaultBucket(String minioFileName) {
        deleteFile(minioProperties.getDefaultBucketName(), minioFileName);
    }

    /**
     * 获取默认桶中文件的元数据信息
     *
     * @param minioFileName MinIO 中的对象文件名
     * @return 文件的元数据
     */
    public StatObjectResponse getFileInfoOnDefaultBucket(String minioFileName) {
        return getFileInfo(minioProperties.getDefaultBucketName(), minioFileName);
    }

    /**
     * 递归列出默认桶中的所有文件
     *
     * @return 包含文件信息的 Item 列表
     */
    public List<Item> listFilesOnDefaultBucket() {
        return listFiles(minioProperties.getDefaultBucketName());
    }

    /**
     * 获取默认桶中文件的临时访问 URL
     *
     * @param minioFileName MinIO 中的对象文件名
     * @param expireTime    过期时间数值
     * @param timeUnit      时间单位
     * @return 临时访问 URL
     */
    public String getFileTempUrlOnDefaultBucket(String minioFileName, int expireTime, TimeUnit timeUnit) {
        return getFileTempUrl(minioProperties.getDefaultBucketName(), minioFileName, expireTime, timeUnit);
    }

    /**
     * 拼接并获取默认桶中文件的公共访问 URL
     *
     * @param minioFileName MinIO 中的对象文件名
     * @return 公共访问 URL
     */
    public String getFileUrlOnDefaultBucket(String minioFileName) {
        return getFileUrl(minioProperties.getDefaultBucketName(), minioFileName);
    }
}