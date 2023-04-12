package com.foghost.logstash.plugin.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * @author zhangwenfeng on 2023/4/11.
 */
public class WebHdfsReader {

    private static final Logger LOGGER = LogManager.getLogger(WebHdfsReader.class);
    private static final String OP_LIST = "LISTSTATUS";
    private static final String OP_OPEN = "OPEN";

    private final String rootUrl;
    private final String userName;
    private final LinkedList<WebHdfsRes.File> files = new LinkedList<>();
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
                    final InputStream inputStream = readFile(curr.getPathSuffix());
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
        final List<WebHdfsRes.File> files = listDir(rootUrl + dir);
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
        return file.getLength() > 0;
    }

    private String path(String a, String b) {
        return a.endsWith("/") ? (a + b) : (a + "/" + b);
    }

    protected InputStream readFile(String path) throws IOException {
        LOGGER.info("read file:" + path);
        Map<String, String> params = new HashMap<>();
        params.put("op", OP_OPEN);
        params.put("user.name", userName);
        final HttpURLConnection urlConnection = httpGet(rootUrl + path, params);
        if (urlConnection.getResponseCode() == 200) {
            return new BufferedInputStream(urlConnection.getInputStream());
        }
        throw new IOException("bad http status code " + urlConnection.getResponseCode() + " url:" + path);
    }

    protected List<WebHdfsRes.File> listDir(String path) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("op", OP_LIST);
        params.put("user.name", userName);
        final HttpURLConnection urlConnection = httpGet(path, params);
        if (urlConnection.getResponseCode() == 200) {
            InputStream in = urlConnection.getInputStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            final String data = reader.lines().collect(Collectors.joining());
            final List<WebHdfsRes.File> list = JsonUtil.toObject(data, WebHdfsRes.class).getFileStatuses().getFileStatus();
            LOGGER.info("list files size:" + list.size() + " dir:" + path);
            return list;
        }
        throw new IOException("bad http status code " + urlConnection.getResponseCode() + " url:" + path);
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
