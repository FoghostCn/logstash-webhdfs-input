package com.foghost.logstash.plugin.input;

import co.elastic.logstash.api.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * @author foghost on 2023/4/11.
 */
@LogstashPlugin(name = "web_hdfs")
public class WebHdfs implements Input {
    public static final PluginConfigSpec<String> URL_CONFIG = PluginConfigSpec.requiredStringSetting("url");
    public static final PluginConfigSpec<String> USER_CONFIG = PluginConfigSpec.requiredStringSetting("user");
    public static final PluginConfigSpec<Codec> CODEC_CONFIG = PluginConfigSpec.codecSetting("codec", "java_line");

    private final Logger logger;
    private String id;
    protected String url;
    protected String userName;
    protected Codec codec;
    private int count = 0;
    protected final CountDownLatch done = new CountDownLatch(1);
    protected final WebHdfsReader reader;

    public WebHdfs(String id, Configuration config, Context context) {
        this.logger = context != null ? context.getLogger(this) : LogManager.getLogger(this.getClass());
        this.id = (id == null || id.isEmpty()) ? UUID.randomUUID().toString() : id;
        this.url = config.get(URL_CONFIG);
        this.userName = config.get(USER_CONFIG);
        this.codec = config.get(CODEC_CONFIG);
        this.reader = new WebHdfsReader(url, userName);
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        try {
            reader.read((file, inputStream) -> {
                try {
                    final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(file.getLength());
                    int c = 0;
                    while (c++ < file.getLength()) {
                        byteBuffer.put((byte) inputStream.read());
                    }
                    byteBuffer.flip();
                    codec.decode(byteBuffer, map -> {
                        count++;
                        consumer.accept(map);
                    });
                } catch (IOException e) {
                    logger.error("read file error " + file.getPathSuffix(), e);
                    throw new RuntimeException(e);
                }
            });
        } finally {
            done.countDown();
            if (logger.isInfoEnabled()) {
                logger.info("total docs read from hdfs: " + count);
            }
        }
    }

    @Override
    public void stop() {
        this.reader.close();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await(); // blocks until input has stopped
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(CODEC_CONFIG, URL_CONFIG, USER_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }
}