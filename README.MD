# logstash webhdfs input plugin

### 安装步骤

将logstash仓库克隆到本地 https://github.com/elastic/logstash.git

在本项目根目录新建gradle.properties文件，内容为 `LOGSTASH_CORE_PATH=pathtologstsah/logstash/logstash-core`

依赖及编译步骤：https://www.elastic.co/guide/en/logstash/current/java-output-plugin.html

修改logstash 根目录 Gemfile 内容为 `source "https://mirrors.tuna.tsinghua.edu.cn/rubygems/"`

打包`./gradlew clean gem`

安装：`bin/logstash-plugin install --no-verify --local /path/to/javaPlugin.gem`

启动logstash：`./bin/logstash -f /path/to/logstash.conf`

查询插件列表：`./bin/logstash-plugin list`

删除插件：`bin/logstash-plugin remove plugin-name`
