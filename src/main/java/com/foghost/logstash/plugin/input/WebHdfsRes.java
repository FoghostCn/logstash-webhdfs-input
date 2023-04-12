package com.foghost.logstash.plugin.input;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class WebHdfsRes {

    @JsonProperty("FileStatuses")
    private FileStatuses fileStatuses;

    public FileStatuses getFileStatuses() {
        return fileStatuses;
    }

    public void setFileStatuses(FileStatuses fileStatuses) {
        this.fileStatuses = fileStatuses;
    }

    public static class FileStatuses {

        @JsonProperty("FileStatus")
        private List<File> fileStatus;

        public List<File> getFileStatus() {
            return fileStatus;
        }

        public void setFileStatus(List<File> fileStatus) {
            this.fileStatus = fileStatus;
        }
    }

    public static class File {

        public static final String TYPE_FILE = "FILE";
        public static final String TYPE_DIR = "DIRECTORY";

        private String owner;
        private int replication;
        private String pathSuffix;
        private long modificationTime;
        private int length;
        private String permission;
        private String type;
        private int blockSize;
        private String group;
        private long accessTime;

        public String getOwner() {
            return owner;
        }

        public int getReplication() {
            return replication;
        }

        public String getPathSuffix() {
            return pathSuffix;
        }

        public long getModificationTime() {
            return modificationTime;
        }

        public int getLength() {
            return length;
        }

        public String getPermission() {
            return permission;
        }

        public String getType() {
            return type;
        }

        public int getBlockSize() {
            return blockSize;
        }

        public String getGroup() {
            return group;
        }

        public long getAccessTime() {
            return accessTime;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        public void setReplication(int replication) {
            this.replication = replication;
        }

        public void setPathSuffix(String pathSuffix) {
            this.pathSuffix = pathSuffix;
        }

        public void setModificationTime(long modificationTime) {
            this.modificationTime = modificationTime;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public void setPermission(String permission) {
            this.permission = permission;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setBlockSize(int blockSize) {
            this.blockSize = blockSize;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public void setAccessTime(long accessTime) {
            this.accessTime = accessTime;
        }
    }

}
