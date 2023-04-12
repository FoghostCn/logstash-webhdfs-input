package com.foghost.logstash.plugin.input;

import co.elastic.logstash.api.*;
import com.fasterxml.jackson.databind.util.ByteBufferBackedOutputStream;
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

    private final String id;
    protected String url;
    protected String userName;
    protected Codec codec;
    protected final CountDownLatch done = new CountDownLatch(1);
    protected final WebHdfsReader reader;
    private final Logger logger;

    public WebHdfs(String id, Configuration config, Context context) {
        this.id = (id == null || id.isEmpty()) ? UUID.randomUUID().toString() : id;
        this.url = config.get(URL_CONFIG);
        this.userName = config.get(USER_CONFIG);
        this.codec = config.get(CODEC_CONFIG);
        this.reader = new WebHdfsReader(url, userName);
        this.logger = context != null ? context.getLogger(this) : LogManager.getLogger(this.getClass());
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        try {
            reader.read((file, inputStream) -> {
                try {
                    final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(file.getLength());
                    try (ByteBufferBackedOutputStream outputStream = new ByteBufferBackedOutputStream(byteBuffer)) {
                        int b = inputStream.read();
                        while (b != -1) {
                            outputStream.write((byte)b);
                            b = inputStream.read();
                        }
                    }
                    byteBuffer.flip();
                    logger.info("read file:" + file.getPathSuffix() + " length:" + file.getLength() + " real length:" + byteBuffer.remaining());
                    codec.decode(byteBuffer, consumer);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            done.countDown();
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