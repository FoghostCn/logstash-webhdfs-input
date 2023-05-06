package com.foghost.logstash.plugin.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * @author zhangwenfeng on 2023/2/10.
 */
public class WebHdfsReader {

    private static final Logger LOGGER = LogManager.getLogger(WebHdfsReader.class);
    private static final String OP_LIST = "LISTSTATUS";
    private static final String OP_OPEN = "OPEN";

    private final String rootUrl;
    private final String userName;
    private final LinkedList<WebHdfsRes.File> files = new LinkedList<>();
    private final int retryTime = -1;
    private boolean close = false;

    public WebHdfsReader(String rootUrl, String userName) {
        this.rootUrl = rootUrl;
        this.userName = userName;
    }

    public void read(BiConsumer<WebHdfsRes.File, InputStream> consumer) {
        try {
            listAllFiles("/");
            WebHdfsRes.File curr;
            while (!close && (curr = files.poll()) != null) {
                try {
                    final InputStream inputStream = readFile(curr);
                    consumer.accept(curr, inputStream);
                } catch (IOException e) {
                    files.add(curr);
                    LOGGER.error("read file error " + curr.getPathSuffix(), e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void listAllFiles(String dir) throws IOException {
        final List<WebHdfsRes.File> files = listDirAndWait(dir);
        for (WebHdfsRes.File file : files) {
            if (WebHdfsRes.File.TYPE_FILE.equals(file.getType())) {
                file.setPathSuffix(path(dir, file.getPathSuffix()));
                if (canRead(file)) {
                    this.files.add(file);
                }
            } else if (WebHdfsRes.File.TYPE_DIR.equals(file.getType())) {
                listAllFiles(path(dir, file.getPathSuffix()));
            } else {
                throw new IOException("unknown file type " + file.getType());
            }
        }
    }

    protected boolean canRead(WebHdfsRes.File file) {
        return WebHdfsRes.File.TYPE_FILE.equals(file.getType()) && file.getLength() > 0;
    }

    private String path(String a, String b) {
        return a.endsWith("/") ? (a + b) : (a + "/" + b);
    }

    protected InputStream readFile(WebHdfsRes.File file) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("read file:" + file.getPathSuffix() + " size:" + file.getLength());
        }
        Map<String, String> params = new HashMap<>();
        params.put("op", OP_OPEN);
        params.put("user.name", userName);
        final HttpURLConnection urlConnection = httpGet(rootUrl + file.getPathSuffix(), params);
        if (urlConnection.getResponseCode() == 200) {
            return new BufferedInputStream(urlConnection.getInputStream());
        }
        throw new IOException("bad http status code " + urlConnection.getResponseCode() + " url:" + file);
    }

    protected List<WebHdfsRes.File> listDir(String path) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("op", OP_LIST);
        params.put("user.name", userName);
        final HttpURLConnection urlConnection = httpGet(rootUrl + path, params);
        if (urlConnection.getResponseCode() == 200) {
            InputStream in = urlConnection.getInputStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            final String data = reader.lines().collect(Collectors.joining());
            final List<WebHdfsRes.File> list = JsonUtil.toObject(data, WebHdfsRes.class).getFileStatuses().getFileStatus();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("list files size:" + list.size() + " dir:" + path);
            }
            return list;
        }
        throw new IOException("bad http status code " + urlConnection.getResponseCode() + " url:" + path);
    }

    protected List<WebHdfsRes.File> listDirAndWait(String path) {
        int retry = retryTime;
        while (retry-- != 0) {
            try {
                final List<WebHdfsRes.File> files = listDir(path);
                final Optional<WebHdfsRes.File> end = files.stream().filter(file -> file.getPathSuffix().endsWith("end")).findAny();
                if (end.isPresent()) {
                    return files;
                }
            } catch (IOException e) {
                LOGGER.error("list dir " + path + " error:" + e.getMessage());
            }
            try {
                int retryTime = 5_000;
                LOGGER.error("list dir " + path + " failed, retry at " + retryTime + "ms later");
                Thread.sleep(retryTime);
            } catch (InterruptedException ignored) {
            }
        }
        LOGGER.error("list dir " + path + " failed after retry " + retryTime + " times");
        return Collections.emptyList();
    }

    protected HttpURLConnection httpGet(String path, Map<String, String> params) throws IOException {
        final String param = params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining("&"));
        URL url = new URL(path + "?" + param);
        final HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setConnectTimeout(5_000);
        urlConnection.setReadTimeout(300_000);
        urlConnection.connect();
        return urlConnection;
    }

    public void close() {
        this.close = true;
    }

}
