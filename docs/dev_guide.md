# 插件开发教程

## 安装jdk

* 1、下载安装es运行版本（如7.4.2）对应的jdk，版本必须严格对应，否则gradle编译会失败

## 安装gradle

* 1、下载安装gradle最新稳定版本
* 2、国内用aliyun的maven源，修改```${GRADLE_HOME}/init.gradle```增加以下内容

```
allprojects{
    repositories {
        def ALIYUN_REPOSITORY_URL = 'https://maven.aliyun.com/nexus/content/groups/public'
        def ALIYUN_JCENTER_URL = 'https://maven.aliyun.com/nexus/content/repositories/jcenter'
        def GRADLE_LOCAL_RELEASE_URL = 'https://repo.gradle.org/gradle/libs-releases-local'
        def ALIYUN_SPRING_RELEASE_URL = 'https://maven.aliyun.com/repository/spring-plugin'
        def ALIYUN_OPENJDK_RELEASE_URL = 'https://glavo-mirrors.oss-cn-beijing.aliyuncs.com/AdoptOpenJDK'

        all { ArtifactRepository repo ->
            if(repo instanceof MavenArtifactRepository){
                def url = repo.url.toString()
                if (url.startsWith('https://repo1.maven.org/maven2')) {
                    project.logger.lifecycle "Repository ${repo.url} replaced by $ALIYUN_REPOSITORY_URL."
                    remove repo
                }
                if (url.startsWith('https://jcenter.bintray.com/')) {
                    project.logger.lifecycle "Repository ${repo.url} replaced by $ALIYUN_JCENTER_URL."
                    remove repo
                }
                if (url.startsWith('http://repo.spring.io/plugins-release')) {
                    project.logger.lifecycle "Repository ${repo.url} replaced by $ALIYUN_SPRING_RELEASE_URL."
                    remove repo
                }
                if (url.startsWith('https://artifactory.elstc.co/artifactory/oss-jdk-local/adoptopenjdk')) {
                    project.logger.lifecycle "Repository ${repo.url} replaced by $ALIYUN_OPENJDK_RELEASE_URL."
                    remove repo
                }
            }
        }
        maven {
            url ALIYUN_REPOSITORY_URL
        }
        maven {
            url ALIYUN_JCENTER_URL
        }
        maven {
            url ALIYUN_SPRING_RELEASE_URL
        }
        maven {
            url GRADLE_LOCAL_RELEASE_URL
        }
        maven {
            url ALIYUN_OPENJDK_RELEASE_URL
        }
    }
}
```

## 源码部署es

* 1、下载es运行版本如[7.4.2](https://www.elastic.co/cn/downloads/past-releases/elasticsearch-7-4-2)的mac版本
* 2、下载es源码切换到对应运行版本的tag分支如[7.4.2](https://github.com/elastic/elasticsearch.git)
* 3、参考[教程](https://www.jetbrains.com/help/idea/gradle.html)，使用idea导入es源码
* 4、编辑es配置文件```/path/to/es_run_config/elasticsearch.yml```例如：

```
# ---------------------------------- Cluster -----------------------------------
#
# Use a descriptive name for your cluster:
#
cluster.name: my-application
#
# ------------------------------------ Node ------------------------------------
#
# Use a descriptive name for the node:
#
node.name: node-1
#
# Add custom attributes to the node:
#
node.attr.rack: r1
#
# ----------------------------------- Paths ------------------------------------
#
# Path to directory where to store the data (separate multiple locations by comma):
#
path.data: /path/to/es_run_data/
#
# Path to log files:
#
path.logs: /path/to/es_run_logs
```

* 5、源码运行es，可以使用以下配置：
```
Main class:
org.elasticsearch.bootstrap.Elasticsearch

VM options:
-Des.path.home=/path/to/es_run_home/ -Des.path.conf=/path/to/es_run_config/ -Dlog4j2.disable.jmx=true -Des.path.data=/path/to/es_run_data/ -Des.path.logs=/path/to/es_run_logs

Use classpath of module:
elasticsearch.server.main

【enabled】 include dependencies with "Provided" scope
```

## 导入和编译插件

* 1、idea创建gradle项目，按照[教程](https://www.jetbrains.com/help/idea/gradle.html)导入或者编辑好gradle项目结构
* 2、插件目录工作下执行```gradle clean build```，成功后会生成安装包```build/distributions/elasticsearch-aggregation-res-topk-7.4.2.0.zip```

## es加载插件

* 1、清理老插件：在es的运行版本目录下执行```./${ES_RUN_HOME}/bin/elasticsearch-plugin remove elasticsearch-aggregation-res-topk```
* 2、安装刚编译的版本：```./${ES_RUN_HOME}/bin/elasticsearch-plugin install file:/path/to/elasticsearch-aggregation-res-topk/build/distributions/elasticsearch-aggregation-res-topk-7.4.2.0.zip```
* 3、重启es