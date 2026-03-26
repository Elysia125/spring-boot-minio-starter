package cn.edu.csu.async;

import cn.edu.csu.MinioFileOperateUtils;
import io.minio.StatObjectResponse;
import io.minio.messages.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * MinIO 异步文件操作工具类
 * <p>
 * 本类是对 {@link MinioFileOperateUtils} 的全异步非阻塞封装。<br>
 * 所有涉及到网络 I/O 的方法均会在独立的线程池中异步执行，方法调用会立即返回 {@link CompletableFuture} 对象，
 * 不会阻塞主线程（如 Tomcat 的 HTTP 工作线程）。<br>
 * 强烈建议在需要高并发、大文件上传或追求接口极速响应的场景下使用本类。
 * </p>
 *
 * @author Elysia
 */
@Component
public class MinioAsyncFileOperateUtils {

    private final MinioFileOperateUtils syncUtils;
    private final Executor executor;

    /**
     * 构造函数注入同步工具类以及执行异步任务的线程池
     *
     * @param syncUtils 同步操作工具类
     * @param executor  Spring 默认任务线程池 (注入名为 applicationTaskExecutor 的 Bean，若无配置则回退到 ForkJoinPool.commonPool())
     */
    public MinioAsyncFileOperateUtils(MinioFileOperateUtils syncUtils,
                                      @Autowired(required = false) @Qualifier("applicationTaskExecutor") Executor executor) {
        this.syncUtils = syncUtils;
        this.executor = executor != null ? executor : ForkJoinPool.commonPool();
    }

    // 桶操作

    /**
     * 异步创建桶。如果桶不存在则创建，并可根据参数设置为允许匿名访问（公共读）。
     *
     * @param bucketName 桶名称
     * @param isPublic   是否设置为公共访问 (true: 公共读, false: 私有)
     * @return CompletableFuture&lt;Void&gt; 异步任务句柄
     */
    public CompletableFuture<Void> createBucket(String bucketName, boolean isPublic) {
        return CompletableFuture.runAsync(() -> syncUtils.createBucket(bucketName, isPublic), executor);
    }

    // 文件上传

    /**
     * 异步上传本地文件到指定桶
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param localFilePath 本地文件的绝对路径
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含上传成功后文件的公共访问 URL
     */
    public CompletableFuture<String> uploadFile(String bucketName, String minioFileName, String localFilePath) {
        return CompletableFuture.supplyAsync(() -> syncUtils.uploadFile(bucketName, minioFileName, localFilePath), executor);
    }

    /**
     * 异步上传 File 对象到指定桶
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param file          本地 File 对象
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含上传成功后文件的公共访问 URL
     */
    public CompletableFuture<String> uploadFile(String bucketName, String minioFileName, File file) {
        return CompletableFuture.supplyAsync(() -> syncUtils.uploadFile(bucketName, minioFileName, file), executor);
    }

    /**
     * 异步流式上传（推荐前端接口传来的文件使用此方法，传入精确大小和类型）
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
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含上传成功后文件的公共访问 URL
     */
    public CompletableFuture<String> putFile(String bucketName, String minioFileName, InputStream inputStream, long objectSize, String contentType) {
        return CompletableFuture.supplyAsync(() -> syncUtils.putFile(bucketName, minioFileName, inputStream, objectSize, contentType), executor);
    }

    /**
     * 异步流式上传（未知大小）：适用于完全未知大小、无特定格式要求的流式数据。<br>
     * 内部强制使用 10MB 分片大小进行分片上传。
     *
     * @param bucketName    桶名称
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param inputStream   数据输入流
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含上传成功后文件的公共访问 URL
     */
    public CompletableFuture<String> putFile(String bucketName, String minioFileName, InputStream inputStream) {
        return CompletableFuture.supplyAsync(() -> syncUtils.putFile(bucketName, minioFileName, inputStream), executor);
    }

    /**
     * 异步批量上传文件到指定桶
     *
     * @param bucketName 目标桶名称
     * @param nameToPath Map 集合，Key 为 MinIO 文件名，Value 为本地文件绝对路径
     * @return CompletableFuture&lt;Map&lt;String, String&gt;&gt; 异步任务句柄，包含成功上传的文件名与对应的 URL 映射
     */
    public CompletableFuture<Map<String, String>> batchUploadFile(String bucketName, Map<String, String> nameToPath) {
        return CompletableFuture.supplyAsync(() -> syncUtils.batchUploadFile(bucketName, nameToPath), executor);
    }

    // 文件下载与读取

    /**
     * 异步从指定桶将文件下载至服务器本地磁盘
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @param localSavePath 本地保存的绝对路径（需包含文件名）
     * @return CompletableFuture&lt;Void&gt; 异步任务句柄
     */
    public CompletableFuture<Void> downloadFile(String bucketName, String minioFileName, String localSavePath) {
        return CompletableFuture.runAsync(() -> syncUtils.downloadFile(bucketName, minioFileName, localSavePath), executor);
    }

    /**
     * 异步获取文件的数据输入流（适用于后端直接向前端返回文件字节流，实现零落盘下载）。
     * <p>
     * <b>极其重要：</b> 在通过 Future 的回调函数（如 {@code thenAccept}）获取到 {@link InputStream} 后，
     * 读取完毕必须手动调用 {@code close()} 方法关闭输入流，否则将导致连接和内存泄漏！
     * </p>
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @return CompletableFuture&lt;InputStream&gt; 异步任务句柄，包含文件的输入流
     */
    public CompletableFuture<InputStream> getFileStream(String bucketName, String minioFileName) {
        return CompletableFuture.supplyAsync(() -> syncUtils.getFileStream(bucketName, minioFileName), executor);
    }

    // 文件信息与管理

    /**
     * 异步从指定桶中删除文件
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @return CompletableFuture&lt;Void&gt; 异步任务句柄
     */
    public CompletableFuture<Void> deleteFile(String bucketName, String minioFileName) {
        return CompletableFuture.runAsync(() -> syncUtils.deleteFile(bucketName, minioFileName), executor);
    }

    /**
     * 异步获取文件的元数据信息（可用于判断文件是否存在、获取文件大小、ContentType 等）
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @return CompletableFuture&lt;StatObjectResponse&gt; 异步任务句柄，包含文件元数据的响应对象
     */
    public CompletableFuture<StatObjectResponse> getFileInfo(String bucketName, String minioFileName) {
        return CompletableFuture.supplyAsync(() -> syncUtils.getFileInfo(bucketName, minioFileName), executor);
    }

    /**
     * 异步递归列出指定桶中的所有文件（包括子目录下的文件）
     *
     * @param bucketName 桶名称
     * @return CompletableFuture&lt;List&lt;Item&gt;&gt; 异步任务句柄，包含文件信息的 Item 列表
     */
    public CompletableFuture<List<Item>> listFiles(String bucketName) {
        return CompletableFuture.supplyAsync(() -> syncUtils.listFiles(bucketName), executor);
    }

    /**
     * 异步生成文件的临时（预签名）访问 URL，适用于私有桶的文件分享与下载。
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @param expireTime    过期时间数值
     * @param timeUnit      时间单位（如 TimeUnit.HOURS）
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含带有预签名的临时访问 URL 字符串
     */
    public CompletableFuture<String> getFileTempUrl(String bucketName, String minioFileName, int expireTime, TimeUnit timeUnit) {
        return CompletableFuture.supplyAsync(() -> syncUtils.getFileTempUrl(bucketName, minioFileName, expireTime, timeUnit), executor);
    }

    /**
     * 仅拼接并获取已有文件的公共访问 URL，不涉及任何底层网络上传或验证操作。<br>
     * （该操作本身不耗时，但为了与本类整体风格统一，依然包装为 Future 返回）
     *
     * @param bucketName    桶名称
     * @param minioFileName MinIO 中的对象文件名
     * @return CompletableFuture&lt;String&gt; 已完成的异步任务句柄，包含公共访问 URL
     */
    public CompletableFuture<String> getFileUrl(String bucketName, String minioFileName) {
        return CompletableFuture.completedFuture(syncUtils.getFileUrl(bucketName, minioFileName));
    }


    // 默认桶快捷操作
    // 以下方法均是对上方基础方法的封装，固定操作 application.yml 中配置的 defaultBucketName

    /**
     * 异步上传本地文件到默认桶
     *
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param localFilePath 本地文件的绝对路径
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含公共访问 URL
     */
    public CompletableFuture<String> uploadFileOnDefaultBucket(String minioFileName, String localFilePath) {
        return CompletableFuture.supplyAsync(() -> syncUtils.uploadFileOnDefaultBucket(minioFileName, localFilePath), executor);
    }

    /**
     * 异步上传本地 File 对象到默认桶
     *
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param file          本地 File 对象
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含公共访问 URL
     */
    public CompletableFuture<String> uploadFileOnDefaultBucket(String minioFileName, File file) {
        return CompletableFuture.supplyAsync(() -> syncUtils.uploadFileOnDefaultBucket(minioFileName, file), executor);
    }

    /**
     * 异步将输入流按指定大小和类型上传到默认桶
     *
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param inputStream   数据输入流
     * @param objectSize    精确字节大小，未知传 -1
     * @param contentType   文件 MIME 类型
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含公共访问 URL
     */
    public CompletableFuture<String> putFileOnDefaultBucket(String minioFileName, InputStream inputStream, long objectSize, String contentType) {
        return CompletableFuture.supplyAsync(() -> syncUtils.putFileOnDefaultBucket(minioFileName, inputStream, objectSize, contentType), executor);
    }

    /**
     * 异步将未知大小的输入流上传到默认桶
     *
     * @param minioFileName 上传到 MinIO 后的对象文件名
     * @param inputStream   数据输入流
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含公共访问 URL
     */
    public CompletableFuture<String> putFileOnDefaultBucket(String minioFileName, InputStream inputStream) {
        return CompletableFuture.supplyAsync(() -> syncUtils.putFileOnDefaultBucket(minioFileName, inputStream), executor);
    }

    /**
     * 异步批量上传文件到默认桶
     *
     * @param nameToPath Map 集合，Key 为 MinIO 文件名，Value 为本地文件绝对路径
     * @return CompletableFuture&lt;Map&lt;String, String&gt;&gt; 异步任务句柄，包含成功上传的文件名与公共访问 URL 映射
     */
    public CompletableFuture<Map<String, String>> batchUploadFileOnDefaultBucket(Map<String, String> nameToPath) {
        return CompletableFuture.supplyAsync(() -> syncUtils.batchUploadFileOnDefaultBucket(nameToPath), executor);
    }

    /**
     * 异步从默认桶下载文件到本地路径
     *
     * @param minioFileName MinIO 中的对象文件名
     * @param localSavePath 本地保存的绝对路径
     * @return CompletableFuture&lt;Void&gt; 异步任务句柄
     */
    public CompletableFuture<Void> downloadFileOnDefaultBucket(String minioFileName, String localSavePath) {
        return CompletableFuture.runAsync(() -> syncUtils.downloadFileOnDefaultBucket(minioFileName, localSavePath), executor);
    }

    /**
     * 异步获取默认桶中文件的输入流（需在回调中手动关闭流）
     *
     * @param minioFileName MinIO 中的对象文件名
     * @return CompletableFuture&lt;InputStream&gt; 异步任务句柄，包含文件的输入流
     */
    public CompletableFuture<InputStream> getFileStreamOnDefaultBucket(String minioFileName) {
        return CompletableFuture.supplyAsync(() -> syncUtils.getFileStreamOnDefaultBucket(minioFileName), executor);
    }

    /**
     * 异步从默认桶删除文件
     *
     * @param minioFileName MinIO 中的对象文件名
     * @return CompletableFuture&lt;Void&gt; 异步任务句柄
     */
    public CompletableFuture<Void> deleteFileOnDefaultBucket(String minioFileName) {
        return CompletableFuture.runAsync(() -> syncUtils.deleteFileOnDefaultBucket(minioFileName), executor);
    }

    /**
     * 异步获取默认桶中文件的元数据信息
     *
     * @param minioFileName MinIO 中的对象文件名
     * @return CompletableFuture&lt;StatObjectResponse&gt; 异步任务句柄，包含文件的元数据
     */
    public CompletableFuture<StatObjectResponse> getFileInfoOnDefaultBucket(String minioFileName) {
        return CompletableFuture.supplyAsync(() -> syncUtils.getFileInfoOnDefaultBucket(minioFileName), executor);
    }

    /**
     * 异步递归列出默认桶中的所有文件
     *
     * @return CompletableFuture&lt;List&lt;Item&gt;&gt; 异步任务句柄，包含文件信息的 Item 列表
     */
    public CompletableFuture<List<Item>> listFilesOnDefaultBucket() {
        return CompletableFuture.supplyAsync(syncUtils::listFilesOnDefaultBucket, executor);
    }

    /**
     * 异步获取默认桶中文件的临时访问 URL
     *
     * @param minioFileName MinIO 中的对象文件名
     * @param expireTime    过期时间数值
     * @param timeUnit      时间单位
     * @return CompletableFuture&lt;String&gt; 异步任务句柄，包含临时访问 URL
     */
    public CompletableFuture<String> getFileTempUrlOnDefaultBucket(String minioFileName, int expireTime, TimeUnit timeUnit) {
        return CompletableFuture.supplyAsync(() -> syncUtils.getFileTempUrlOnDefaultBucket(minioFileName, expireTime, timeUnit), executor);
    }

    /**
     * 异步拼接并获取默认桶中文件的公共访问 URL（不耗时）
     *
     * @param minioFileName MinIO 中的对象文件名
     * @return CompletableFuture&lt;String&gt; 已完成的异步任务句柄，包含公共访问 URL
     */
    public CompletableFuture<String> getFileUrlOnDefaultBucket(String minioFileName) {
        return CompletableFuture.completedFuture(syncUtils.getFileUrlOnDefaultBucket(minioFileName));
    }
}
