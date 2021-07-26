/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code DefaultContainerExecuter} class offers generic container
 * execution services. Process execution is handled in a platform-independent
 * way via {@link ProcessBuilder}.
 */
// todo 简称DCE。每个Container运行在单独的进程里，但进程都是由NM的用户启动的。
//  比如NM进程是用yarn用户启动的，那么所有Container的进程也由yarn用户启动。
//
// todo 在ContainerExecutor启动一个Container的过程中，涉及到了三个脚本，它们分别是：
//    1.default_container_executor.sh
//    2.default_container_executor_session.sh
//    3.launch_container.sh
//    这三个脚本，都是跟Container相关的，所以它们都被放在一个Container所代表的目录结构下。
//    在NodeManager中，会为了每个Application，以及每个Container建立一个对应的目录，
//    在每个Container的目录下，就放置了一些运行这个Container所必须的信息。
//    一般来说，这些目录是位于/tmp这个目录下，并且会在一个Application完成后，被删除，减少磁盘空间的消耗。

/**
 * tree /tmp/data1:
 *    /tmp/data1
 *    |———— dfs
 *    |     |—— namesecondary
 *    |         |———— in_user.lock
 *    |———— nm-local-dir
 *          |—— filecache
 *          |—— nmPrivate
 *          |—— usercache
 *              |—— tenant1
 *                  |—— appcache
 *                  |   |—— application_1520752729961_0001
 *                  |       |—— container_1520752729961_0001_01_000001
 *                  |       |   |—— container_tokens
 *                  |       |   |—— default_container_executor_session.sh
 *                  |       |   |—— default_container_executor.sh
 *                  |       |   |—— launch_container.sh
 *                  |       |   |—— job.jar -> /tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001/filecache/11/job.jar
 *                  |       |   |—— jobSubmitDir
 *                  |       |   |   |—— job.split -> /tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001/filecache/12/job.split
 *                  |       |   |   |—— job.splitmetainfo -> /tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001/filecache/10/job.splitmetainfo
 *                  |       |   |—— job.xml -> /tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001/filecache/13/job.xml
 *                  |       |   |—— tmp
 *                  |       |—— container_1520752729961_0001_01_000002
 *                  |       |   |—— container_tokens
 *                  |       |   |—— default_container_executor_session.sh
 *                  |       |   |—— default_container_executor.sh
 *                  |       |   |—— launch_container.sh
 *                  |       |   |—— job.jar -> /tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001/filecache/11/job.jar
 *                  |       |   |—— job.xml -> /tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001/filecache/13/job.xml
 *                  |       |   |—— tmp
 *
 *  我们分别查看一下，上面所说的三个脚本文件的内容.
 *  default-container_executor.sh:
 *     | #!/bin/bash
 *     | /bin/bash "/tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001/container_1520752729961_0001_01_000001/default_container_executor_session.sh"
 *     | rc=$?
 *     | echo $rc > "/tmp/data1/nm-local-dir/nmPrivate/application_1520752729961_0001/container_1520752729961_0001_01_000001/container_1520752729961_0001_01_000001.pid.exitcode.tmp"
 *     | /bin/mv -f "/tmp/data1/nm-local-dir/nmPrivate/application_1520752729961_0001/container_1520752729961_0001_01_000001/container_1520752729961_0001_01_000001.pid.exitcode.tmp" "/data3/nm-local-dir/nmPrivate/application_1520752729961_0001/container_1520752729961_0001_01_000001/container_1520752729961_0001_01_000001.pid.exitcode"
 *     | exit $rc
 *   可以看到，在这个脚本文件的内部，会启动 default_container_executor_session.sh 这个脚本，并将执行结果写入到这个Container的一个名为Container ID + pid.exitcode的文件中。
 *   而 default_container_executor_session.sh 这个脚本呢？
 *     | #!/bin/bash
 *
 *     | echo $$ > /tmp/data1/nm-local-dir/nmPrivate/application_1520752729961_0001/container_1520752729961_0001_01_000001/container_1520752729961_0001_01_000001.pid.tmp
 *     | /bin/mv -f /tmp/data1/nm-local-dir/nmPrivate/application_1520752729961_0001/container_1520752729961_0001_01_000001/container_1520752729961_0001_01_000001.pid.tmp /data3/nm-local-dir/nmPrivate/application_1520752729961_0001/container_1520752729961_0001_01_000001/container_1520752729961_0001_01_000001.pid
 *     | exec setsid /bin/bash "/tmp/data1/nm-local-dir/usercache/redpeak/appcache/application_1520752729961_0001/container_1520752729961_0001_01_000001/launch_container.sh"
 *   可以看到，它主要是启动launch_container.sh这个脚本
 *   在launch_container.sh中，就负责运行相应的Container，也能是MRAppMaster，也可能是Mapper或者Reducer。
 *     | #!/bin/bash
 *
 *     | export SPARK_YARN_STAGING_DIR="hdfs://hdfs/user/tenant1/.sparkStaging/application_1624160373023_0001"
 *     | export HADOOP_CONF_DIR="/home/hypergalaxy/hadoop/etc/hadoop"
 *     | export JAVA_HOME="/usr/jdk64/jdk1.8.0_112"
 *     | export SPARK_LOG_URL_STDOUT="http://node214:8042/node/containerlogs/container_1520752729961_0001_01_000001/redpeak/stdout?start=-4096"
 *     | export NM_HOST="node214"
 *     | export SPARK_HOME="/home/hypergalaxy/spark"
 *     | export HADOOP_HDFS_HOME="/home/hypergalaxy/hadoop"
 *     | export LOGNAME="redpeak"
 *     | export JVM_PID="$$"
 *     | export PWD="/tmp/data1/nm-local-dir/usercache/redpeak/appcache/application_1520752729961_0001/container_1520752729961_0001_01_000001"
 *     | export HADOOP_COMMON_HOME="/home/hypergalaxy/hadoop"
 *     | export LOCAL_DIRS="/tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001"
 *     | export NM_HTTP_PORT="8042"
 *     | export LOG_DIRS="/tmp/data1/nm-logs-dir/application_1520752729961_0001/container_1520752729961_0001_01_000001"
 *     | export NM_PORT="31887"
 *     | export USER="tenant1"
 *     | export HADOOP_YARN_HOME="/home/hypergalaxy/hadoop"
 *     | export CLASSPATH="$PWD:$PWD/__spark_conf__:$PWD/__spark_libs__/*:$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*"
 *     | export SPARK_YARN_MODE="true"
 *     | export HADOOP_TOKEN_FILE_LOCATION="/tmp/data1/nm-local-dir/usercache/tenant1/appcache/application_1520752729961_0001/container_1520752729961_0001_01_000001/container_tokens"
 *     | export SPARK_USER="tenant1"
 *     | export SPARK_LOG_URL_STDERR="http://node214:8042/node/containerlogs/container_e18_1624160373023_0001_01_000021/redpeak/stderr?start=-4096"
 *     | export HOME="/home/"
 *     | export CONTAINER_ID="container_1520752729961_0001_01_000001"
 *     | export MALLOC_ARENA_MAX="4"
 *     | ln -sf "/tmp/data1/nm-local-dir/usercache/tenant1/filecache/33/__spark_conf__.zip" "__spark_conf__"
 *     | hadoop_shell_errorcode=$?
 *     | if [ $hadoop_shell_errorcode -ne 0 ]
 *     | then
 *     |   exit $hadoop_shell_errorcode
 *     | fi
 *     | ln -sf "/tmp/data1/nm-local-dir/filecache/11/spark2-archive" "__spark_libs__"
 *     | hadoop_shell_errorcode=$?
 *     | if [ $hadoop_shell_errorcode -ne 0 ]
 *     | then
 *     |   exit $hadoop_shell_errorcode
 *     | fi
 *     | exec /bin/bash -c "$JAVA_HOME/bin/java -server -Xmx4096m '-Dlog4j.configuration=file:/home/hypergalaxy/spark/conf/log4j-thriftserver-executor.properties' -Djava.io.tmpdir=$PWD/tmp '-Dspark.master.port=7077' '-Dspark.history.ui.port=18081' '-Dspark.driver.port=28343' -Dspark.yarn.app.container.log.dir=/data3/nm-logs-dir/application_1520752729961_0001/container_e18_1624160373023_0001_01_000021 -XX:OnOutOfMemoryError='kill %p' org.apache.spark.executor.CoarseGrainedExecutorBackend --driver-url spark://CoarseGrainedScheduler@192.168.1.215:28343 --executor-id 20 --hostname node214 --cores 2 --app-id application_1624160373023_0001 --user-class-path file:$PWD/__app__.jar 1>/data3/nm-logs-dir/application_1624160373023_0001/container_e18_1624160373023_0001_01_000021/stdout 2>/data3/nm-logs-dir/application_1624160373023_0001/container_1520752729961_0001_01_000001/stderr"
 *     | hadoop_shell_errorcode=$?
 *     | if [ $hadoop_shell_errorcode -ne 0 ]
 *     | then
 *     |   exit $hadoop_shell_errorcode
 *     | fi
 *
 *  在launch_container.sh中，设置了很多环境变量。
 *  那么，DefaultContainerExecutor应该就是首先执行default_container_executor.sh这个脚本。
 */
public class DefaultContainerExecutor extends ContainerExecutor {

  private static final Logger LOG =
       LoggerFactory.getLogger(DefaultContainerExecutor.class);

  private static final int WIN_MAX_PATH = 260;

  /**
   * A {@link FileContext} for the local file system.
   */
  protected final FileContext lfs;

  private String logDirPermissions = null;

  /**
   * Default constructor for use in testing.
   */
  @VisibleForTesting
  public DefaultContainerExecutor() {
    try {
      this.lfs = FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create an instance with a given {@link FileContext}.
   *
   * @param lfs the given {@link FileContext}
   */
  DefaultContainerExecutor(FileContext lfs) {
    this.lfs = lfs;
  }

  /**
   * Copy a file using the {@link #lfs} {@link FileContext}.
   *
   * @param src the file to copy
   * @param dst where to copy the file
   * @param owner the owner of the new copy. Used only in secure Windows
   * clusters
   * @throws IOException when the copy fails
   * @see WindowsSecureContainerExecutor
   */
  protected void copyFile(Path src, Path dst, String owner) throws IOException {
    lfs.util().copy(src, dst, false, true);
  }
  
  /**
   * Make a file executable using the {@link #lfs} {@link FileContext}.
   *
   * @param script the path to make executable
   * @param owner the new owner for the file. Used only in secure Windows
   * clusters
   * @throws IOException when the change mode operation fails
   * @see WindowsSecureContainerExecutor
   */
  protected void setScriptExecutable(Path script, String owner)
      throws IOException {
    lfs.setPermission(script, ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
  }

  @Override
  public void init(Context nmContext) throws IOException {
    // nothing to do or verify here
  }

  /**
   * todo 设置初始化操作，需要传入初始化对象
   *    LocalizerStartContext：封装启动本地化程序所需的信息
   * @param ctx LocalizerStartContext that encapsulates necessary information
   *            for starting a localizer.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void startLocalizer(LocalizerStartContext ctx)
      throws IOException, InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    // todo 获取nodeManager 通讯地址
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();

    // todo 本地文件目录
    List<String> localDirs = dirsHandler.getLocalDirs();
    // todo 本地日志文件目录
    List<String> logDirs = dirsHandler.getLogDirs();

    // todo 初始化特定用户的本地目录 localDirs
    //  create $local.dir/usercache/$user and its immediate parent
    createUserLocalDirs(localDirs, user);
    // todo 创建用户缓存目录
    //  create $local.dir/usercache/$user/appcache
    createUserCacheDirs(localDirs, user);
    // todo 创建App目录
    //  create $local.dir/usercache/$user/appcache/$appId
    createAppDirs(localDirs, user, appId);
    // todo 创建App 日志目录
    //  create $log.dir/$appid
    createAppLogDirs(appId, logDirs, user);

    // todo 创建工作目录
    //  从本地存储目录列表中返回随机选择的应用程序目录，选择目录的概率与其大小成比例
    // randomly choose the local directory
    Path appStorageDir = getWorkingDir(localDirs, user, appId);

    String tokenFn = String.format(TOKEN_FILE_NAME_FMT, locId);
    Path tokenDst = new Path(appStorageDir, tokenFn);
    // todo 复制文件
    copyFile(nmPrivateContainerTokensPath, tokenDst, user);
    LOG.info("Copying from {} to {}", nmPrivateContainerTokensPath, tokenDst);


    FileContext localizerFc =
        FileContext.getFileContext(lfs.getDefaultFileSystem(), getConf());
    localizerFc.setUMask(lfs.getUMask());
    localizerFc.setWorkingDirectory(appStorageDir);
    LOG.info("Localizer CWD set to {} = {}", appStorageDir,
        localizerFc.getWorkingDirectory());

    ContainerLocalizer localizer =
        createContainerLocalizer(user, appId, locId, tokenFn, localDirs,
            localizerFc);
    // TODO: DO it over RPC for maintaining similarity?
    localizer.runLocalization(nmAddr);
  }

  /**
   * Create a new {@link ContainerLocalizer} instance.
   *
   * @param user the user who owns the job for which the localization is being
   * run
   * @param appId the ID of the application for which the localization is being
   * run
   * @param locId the ID of the container for which the localization is being
   * run
   * @param localDirs a list of directories to use as destinations for the
   * localization
   * @param localizerFc the {@link FileContext} to use when localizing files
   * @return the new {@link ContainerLocalizer} instance
   * @throws IOException if {@code user} or {@code locId} is {@code null} or if
   * the container localizer has an initialization failure
   */
  @Private
  @VisibleForTesting
  protected ContainerLocalizer createContainerLocalizer(String user,
      String appId, String locId, String tokenFileName, List<String> localDirs,
      FileContext localizerFc) throws IOException {
    ContainerLocalizer localizer =
        new ContainerLocalizer(localizerFc, user, appId, locId, tokenFileName,
            getPaths(localDirs),
            RecordFactoryProvider.getRecordFactory(getConf()));
    return localizer;
  }

  /**
   * todo 启动Container
   *   这个实现有一些问题，即，对于资源隔离做的并不好。
   *   全部Container都是由运行NodeManager的那个用户启动的。
   *
   * @param ctx Encapsulates information necessary for launching containers.
   * @return
   * @throws IOException
   * @throws ConfigurationException
   */
  @Override
  public int launchContainer(ContainerStartContext ctx)
      throws IOException, ConfigurationException {
    Container container = ctx.getContainer();
    Path nmPrivateContainerScriptPath = ctx.getNmPrivateContainerScriptPath();
    Path nmPrivateTokensPath = ctx.getNmPrivateTokensPath();
    Path nmPrivateKeystorePath = ctx.getNmPrivateKeystorePath();
    Path nmPrivateTruststorePath = ctx.getNmPrivateTruststorePath();
    String user = ctx.getUser();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();

    FsPermission dirPerm = new FsPermission(APPDIR_PERM);
    ContainerId containerId = container.getContainerId();

    // create container dirs on all disks
    // todo 在所有磁盘上创建 container 目录
    String containerIdStr = containerId.toString();
    String appIdStr =
            containerId.getApplicationAttemptId().
                getApplicationId().toString();
    for (String sLocalDir : localDirs) {
      Path usersdir = new Path(sLocalDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, user);
      Path appCacheDir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(appCacheDir, appIdStr);
      Path containerDir = new Path(appDir, containerIdStr);
      createDir(containerDir, dirPerm, true, user);
    }

    // Create the container log-dirs on all disks
    // todo 在所有硬盘上创建 log 目录
    createContainerLogDirs(appIdStr, containerIdStr, logDirs, user);

    // todo 创建临时文件目录：./tmp
    Path tmpDir = new Path(containerWorkDir,
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    createDir(tmpDir, dirPerm, false, user);


    // copy container tokens to work dir
    Path tokenDst =
      new Path(containerWorkDir, ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
    copyFile(nmPrivateTokensPath, tokenDst, user);

    if (nmPrivateKeystorePath != null) {
      Path keystoreDst =
          new Path(containerWorkDir, ContainerLaunch.KEYSTORE_FILE);
      copyFile(nmPrivateKeystorePath, keystoreDst, user);
    }

    if (nmPrivateTruststorePath != null) {
      Path truststoreDst =
          new Path(containerWorkDir, ContainerLaunch.TRUSTSTORE_FILE);
      copyFile(nmPrivateTruststorePath, truststoreDst, user);
    }

    // copy launch script to work dir
    Path launchDst =
        new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
    copyFile(nmPrivateContainerScriptPath, launchDst, user);

    // Create new local launch wrapper script
    // todo 创建新的本地启动包装脚本
    LocalWrapperScriptBuilder sb = getLocalWrapperScriptBuilder(
        containerIdStr, containerWorkDir); 

    // Fail fast if attempting to launch the wrapper script would fail due to
    // Windows path length limitation.
    if (Shell.WINDOWS &&
        sb.getWrapperScriptPath().toString().length() > WIN_MAX_PATH) {
      throw new IOException(String.format(
        "Cannot launch container using script at path %s, because it exceeds " +
        "the maximum supported path length of %d characters.  Consider " +
        "configuring shorter directories in %s.", sb.getWrapperScriptPath(),
        WIN_MAX_PATH, YarnConfiguration.NM_LOCAL_DIRS));
    }

    Path pidFile = getPidFilePath(containerId);
    if (pidFile != null) {
      // todo 获取pidFile，写入启动脚本
      sb.writeLocalWrapperScript(launchDst, pidFile);
    } else {
      LOG.info("Container {} pid file not set. Returning terminated error",
          containerIdStr);
      return ExitCode.TERMINATED.getExitCode();
    }
    
    // create log dir under app
    // fork script
    Shell.CommandExecutor shExec = null;
    try {
      setScriptExecutable(launchDst, user);
      setScriptExecutable(sb.getWrapperScriptPath(), user);

      shExec = buildCommandExecutor(sb.getWrapperScriptPath().toString(),
          containerIdStr, user, pidFile, container.getResource(),
          new File(containerWorkDir.toUri().getPath()),
          container.getLaunchContext().getEnvironment());

      // todo containerId 如果存活的话，启动命令
      if (isContainerActive(containerId)) {
        // todo ------------- 启动 start -------------
        shExec.execute();
        // todo ------------- 启动 end -------------
      } else {
        LOG.info("Container {} was marked as inactive. "
            + "Returning terminated error", containerIdStr);
        return ExitCode.TERMINATED.getExitCode();
      }
    } catch (IOException e) {
      if (null == shExec) {
        return -1;
      }
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container {} is : {}", containerId, exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
      // terminated/killed forcefully. In all other cases, log the
      // container-executor's output
      if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ExitCode.TERMINATED.getExitCode()) {
        LOG.warn("Exception from container-launch with container ID: {}"
            + " and exit code: {}", containerId, exitCode, e);

        StringBuilder builder = new StringBuilder();
        builder.append("Exception from container-launch.\n")
            .append("Container id: ").append(containerId).append("\n")
            .append("Exit code: ").append(exitCode).append("\n");
        if (!Optional.ofNullable(e.getMessage()).orElse("").isEmpty()) {
          builder.append("Exception message: ")
              .append(e.getMessage()).append("\n");
        }

        if (!shExec.getOutput().isEmpty()) {
          builder.append("Shell output: ")
              .append(shExec.getOutput()).append("\n");
        }
        String diagnostics = builder.toString();
        logOutput(diagnostics);
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            diagnostics));
      } else {
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            "Container killed on request. Exit code is " + exitCode));
      }
      return exitCode;
    } finally {
      if (shExec != null) shExec.close();
    }
    return 0;
  }

  @Override
  public int relaunchContainer(ContainerStartContext ctx)
      throws IOException, ConfigurationException {
    return launchContainer(ctx);
  }

  /**
   * Create a new {@link ShellCommandExecutor} using the parameters.
   *
   * @param wrapperScriptPath the path to the script to execute
   * @param containerIdStr the container ID
   * @param user the application owner's username
   * @param pidFile the path to the container's PID file
   * @param resource this parameter controls memory and CPU limits.
   * @param workDir If not-null, specifies the directory which should be set
   * as the current working directory for the command. If null,
   * the current working directory is not modified.
   * @param environment the container environment
   * @return the new {@link ShellCommandExecutor}
   * @see ShellCommandExecutor
   */
  protected CommandExecutor buildCommandExecutor(String wrapperScriptPath, 
      String containerIdStr, String user, Path pidFile, Resource resource,
      File workDir, Map<String, String> environment) {
    
    String[] command = getRunCommand(wrapperScriptPath,
        containerIdStr, user, pidFile, this.getConf(), resource);

    LOG.info("launchContainer: {}", Arrays.toString(command));
    return new ShellCommandExecutor(
        command,
        workDir,
        environment,
        0L,
        false);
  }

  /**
   * Create a {@link LocalWrapperScriptBuilder} for the given container ID
   * and path that is appropriate to the current platform.
   *
   * @param containerIdStr the container ID
   * @param containerWorkDir the container's working directory
   * @return a new {@link LocalWrapperScriptBuilder}
   */
  protected LocalWrapperScriptBuilder getLocalWrapperScriptBuilder(
      String containerIdStr, Path containerWorkDir) {
   return  Shell.WINDOWS ?
       new WindowsLocalWrapperScriptBuilder(containerIdStr, containerWorkDir) :
       new UnixLocalWrapperScriptBuilder(containerWorkDir);
  }

  /**
   * This class is a utility to create a wrapper script that is platform
   * appropriate.
   */
  protected abstract class LocalWrapperScriptBuilder {

    private final Path wrapperScriptPath;

    /**
     * Return the path for the wrapper script.
     *
     * @return the path for the wrapper script
     */
    public Path getWrapperScriptPath() {
      return wrapperScriptPath;
    }

    /**
     * Write out the wrapper script for the container launch script. This method
     * will create the script at the configured wrapper script path.
     *
     * @param launchDst the script to launch
     * @param pidFile the file that will hold the PID
     * @throws IOException if the wrapper script cannot be created
     * @see #getWrapperScriptPath
     */
    public void writeLocalWrapperScript(Path launchDst, Path pidFile)
        throws IOException {
      try (DataOutputStream out =
               lfs.create(wrapperScriptPath, EnumSet.of(CREATE, OVERWRITE));
           PrintStream pout =
               new PrintStream(out, false, "UTF-8")) {
        writeLocalWrapperScript(launchDst, pidFile, pout);
      }
    }

    /**
     * Write out the wrapper script for the container launch script.
     *
     * @param launchDst the script to launch
     * @param pidFile the file that will hold the PID
     * @param pout the stream to use to write out the wrapper script
     */
    protected abstract void writeLocalWrapperScript(Path launchDst,
        Path pidFile, PrintStream pout);

    /**
     * Create an instance for the given container working directory.
     *
     * @param containerWorkDir the working directory for the container
     */
    protected LocalWrapperScriptBuilder(Path containerWorkDir) {
      this.wrapperScriptPath = new Path(containerWorkDir,
        Shell.appendScriptExtension("default_container_executor"));
    }
  }

  /**
   * This class is an instance of {@link LocalWrapperScriptBuilder} for
   * non-Windows hosts.
   */
  private final class UnixLocalWrapperScriptBuilder
      extends LocalWrapperScriptBuilder {
    private final Path sessionScriptPath;

    /**
     * Create an instance for the given container path.
     *
     * @param containerWorkDir the container's working directory
     */
    public UnixLocalWrapperScriptBuilder(Path containerWorkDir) {
      super(containerWorkDir);
      this.sessionScriptPath = new Path(containerWorkDir,
          Shell.appendScriptExtension("default_container_executor_session"));
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile)
        throws IOException {
      writeSessionScript(launchDst, pidFile);
      super.writeLocalWrapperScript(launchDst, pidFile);
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
        PrintStream pout) {
      String exitCodeFile = ContainerLaunch.getExitCodeFile(
          pidFile.toString());
      String tmpFile = exitCodeFile + ".tmp";
      pout.println("#!/bin/bash");
      pout.println("/bin/bash \"" + sessionScriptPath.toString() + "\"");
      pout.println("rc=$?");
      pout.println("echo $rc > \"" + tmpFile + "\"");
      pout.println("/bin/mv -f \"" + tmpFile + "\" \"" + exitCodeFile + "\"");
      pout.println("exit $rc");
    }

    private void writeSessionScript(Path launchDst, Path pidFile)
        throws IOException {
      try (DataOutputStream out =
               lfs.create(sessionScriptPath, EnumSet.of(CREATE, OVERWRITE));
           PrintStream pout =
               new PrintStream(out, false, "UTF-8")) {
        // We need to do a move as writing to a file is not atomic
        // Process reading a file being written to may get garbled data
        // hence write pid to tmp file first followed by a mv
        pout.println("#!/bin/bash");
        pout.println();
        pout.println("echo $$ > " + pidFile.toString() + ".tmp");
        pout.println("/bin/mv -f " + pidFile.toString() + ".tmp " + pidFile);
        String exec = Shell.isSetsidAvailable? "exec setsid" : "exec";
        pout.printf("%s /bin/bash \"%s\"", exec, launchDst.toUri().getPath());
      }
      lfs.setPermission(sessionScriptPath,
          ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
    }
  }

  /**
   * This class is an instance of {@link LocalWrapperScriptBuilder} for
   * Windows hosts.
   */
  private final class WindowsLocalWrapperScriptBuilder
      extends LocalWrapperScriptBuilder {

    private final String containerIdStr;

    /**
     * Create an instance for the given container and working directory.
     *
     * @param containerIdStr the container ID
     * @param containerWorkDir the container's working directory
     */
    public WindowsLocalWrapperScriptBuilder(String containerIdStr,
        Path containerWorkDir) {

      super(containerWorkDir);
      this.containerIdStr = containerIdStr;
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
        PrintStream pout) {
      // TODO: exit code script for Windows

      // On Windows, the pid is the container ID, so that it can also serve as
      // the name of the job object created by winutils for task management.
      // Write to temp file followed by atomic move.
      String normalizedPidFile = new File(pidFile.toString()).getPath();
      pout.println("@echo " + containerIdStr + " > " + normalizedPidFile +
        ".tmp");
      pout.println("@move /Y " + normalizedPidFile + ".tmp " +
        normalizedPidFile);
      pout.println("@call " + launchDst.toString());
    }
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();
    LOG.debug("Sending signal {} to pid {} as user {}",
        signal.getValue(), pid, user);
    if (!containerIsAlive(pid)) {
      return false;
    }
    try {
      killContainer(pid, signal);
    } catch (IOException e) {
      if (!containerIsAlive(pid)) {
        return false;
      }
      throw e;
    }
    return true;
  }

  /**
   * No-op for reaping containers within the DefaultContainerExecutor.
   *
   * @param ctx Encapsulates information necessary for reaping containers.
   * @return true given no operations are needed.
   */
  @Override
  public boolean reapContainer(ContainerReapContext ctx) {
    return true;
  }

  @Override
  public boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException {
    String pid = ctx.getPid();

    return containerIsAlive(pid);
  }

  /**
   * Returns true if the process with the specified pid is alive.
   * 
   * @param pid String pid
   * @return boolean true if the process is alive
   * @throws IOException if the command to test process liveliness fails
   */
  @VisibleForTesting
  public static boolean containerIsAlive(String pid) throws IOException {
    try {
      new ShellCommandExecutor(Shell.getCheckProcessIsAliveCommand(pid))
        .execute();
      // successful execution means process is alive
      return true;
    }
    catch (ExitCodeException e) {
      // failure (non-zero exit code) means process is not alive
      return false;
    }
  }

  /**
   * Send a specified signal to the specified pid
   *
   * @param pid the pid of the process [group] to signal.
   * @param signal signal to send
   * @throws IOException if the command to kill the process fails
   */
  protected void killContainer(String pid, Signal signal) throws IOException {
    new ShellCommandExecutor(Shell.getSignalKillCommand(signal.getValue(), pid))
      .execute();
  }

  @Override
  public void deleteAsUser(DeletionAsUserContext ctx)
      throws IOException, InterruptedException {
    Path subDir = ctx.getSubDir();
    List<Path> baseDirs = ctx.getBasedirs();

    if (baseDirs == null || baseDirs.size() == 0) {
      LOG.info("Deleting absolute path : {}", subDir);
      if (!lfs.delete(subDir, true)) {
        //Maybe retry
        LOG.warn("delete returned false for path: [{}]", subDir);
      }
      return;
    }
    for (Path baseDir : baseDirs) {
      Path del = subDir == null ? baseDir : new Path(baseDir, subDir);
      LOG.info("Deleting path : {}", del);
      try {
        if (!lfs.delete(del, true)) {
          LOG.warn("delete returned false for path: [{}]", del);
        }
      } catch (FileNotFoundException e) {
        continue;
      }
    }
  }

  @Override
  public void symLink(String target, String symlink) throws IOException {
    FileUtil.symLink(target, symlink);
  }

  /**
   * Permissions for user dir.
   * $local.dir/usercache/$user
   */
  static final short USER_PERM = (short)0750;
  /**
   * Permissions for user appcache dir.
   * $local.dir/usercache/$user/appcache
   */
  static final short APPCACHE_PERM = (short)0710;
  /**
   * Permissions for user filecache dir.
   * $local.dir/usercache/$user/filecache
   */
  static final short FILECACHE_PERM = (short)0710;
  /**
   * Permissions for user app dir.
   * $local.dir/usercache/$user/appcache/$appId
   */
  static final short APPDIR_PERM = (short)0710;

  private long getDiskFreeSpace(Path base) throws IOException {
    return lfs.getFsStatus(base).getRemaining();
  }

  private Path getApplicationDir(Path base, String user, String appId) {
    return new Path(getAppcacheDir(base, user), appId);
  }

  private Path getUserCacheDir(Path base, String user) {
    return new Path(new Path(base, ContainerLocalizer.USERCACHE), user);
  }

  private Path getAppcacheDir(Path base, String user) {
    return new Path(getUserCacheDir(base, user),
        ContainerLocalizer.APPCACHE);
  }

  private Path getFileCacheDir(Path base, String user) {
    return new Path(getUserCacheDir(base, user),
        ContainerLocalizer.FILECACHE);
  }

  /**
   * Return a randomly chosen application directory from a list of local storage
   * directories. The probability of selecting a directory is proportional to
   * its size.
   *
   * @param localDirs the target directories from which to select
   * @param user the user who owns the application
   * @param appId the application ID
   * @return the selected directory
   * @throws IOException if no application directories for the user can be
   * found
   */
  protected Path getWorkingDir(List<String> localDirs, String user,
      String appId) throws IOException {
    long totalAvailable = 0L;
    long[] availableOnDisk = new long[localDirs.size()];
    int i = 0;
    // randomly choose the app directory
    // the chance of picking a directory is proportional to
    // the available space on the directory.
    // firstly calculate the sum of all available space on these directories
    for (String localDir : localDirs) {
      Path curBase = getApplicationDir(new Path(localDir), user, appId);
      long space = 0L;
      try {
        space = getDiskFreeSpace(curBase);
      } catch (IOException e) {
        LOG.warn("Unable to get Free Space for {}", curBase, e);
      }
      availableOnDisk[i++] = space;
      totalAvailable += space;
    }

    // throw an IOException if totalAvailable is 0.
    if (totalAvailable <= 0L) {
      throw new IOException("Not able to find a working directory for " + user);
    }

    // make probability to pick a directory proportional to
    // the available space on the directory.
    long randomPosition = RandomUtils.nextLong() % totalAvailable;
    int dir = pickDirectory(randomPosition, availableOnDisk);

    return getApplicationDir(new Path(localDirs.get(dir)), user, appId);
  }

  /**
   * Picks a directory based on the input random number and
   * available size at each dir.
   */
  @Private
  @VisibleForTesting
  int pickDirectory(long randomPosition, final long[] availableOnDisk) {
    int dir = 0;
    // skip zero available space directory,
    // because totalAvailable is greater than 0 and randomPosition
    // is less than totalAvailable, we can find a valid directory
    // with nonzero available space.
    while (availableOnDisk[dir] == 0L) {
      dir++;
    }
    while (randomPosition >= availableOnDisk[dir]) {
      randomPosition -= availableOnDisk[dir++];
    }
    return dir;
  }

  /**
   * Use the {@link #lfs} {@link FileContext} to create the target directory.
   *
   * @param dirPath the target directory
   * @param perms the target permissions for the target directory
   * @param createParent whether the parent directories should also be created
   * @param user the user as whom the target directory should be created.
   * Used only on secure Windows hosts.
   * @throws IOException if there's a failure performing a file operation
   * @see WindowsSecureContainerExecutor
   */
  protected void createDir(Path dirPath, FsPermission perms,
      boolean createParent, String user) throws IOException {
    lfs.mkdir(dirPath, perms, createParent);
    if (!perms.equals(perms.applyUMask(lfs.getUMask()))) {
      lfs.setPermission(dirPath, perms);
    }
  }

  /**
   * Initialize the local directories for a particular user.
   * <ul>.mkdir
   * <li>$local.dir/usercache/$user</li>
   * </ul>
   *
   * @param localDirs the target directories to create
   * @param user the user whose local cache directories should be initialized
   * @throws IOException if there's an issue initializing the user local
   * directories
   */
  void createUserLocalDirs(List<String> localDirs, String user)
      throws IOException {
    boolean userDirStatus = false;
    FsPermission userperms = new FsPermission(USER_PERM);
    for (String localDir : localDirs) {
      // create $local.dir/usercache/$user and its immediate parent
      try {
        createDir(getUserCacheDir(new Path(localDir), user), userperms, true,
            user);
      } catch (IOException e) {
        LOG.warn("Unable to create the user directory : {}", localDir, e);
        continue;
      }
      userDirStatus = true;
    }
    if (!userDirStatus) {
      throw new IOException("Not able to initialize user directories "
          + "in any of the configured local directories for user " + user);
    }
  }


  /**
   * Initialize the local cache directories for a particular user.
   * <ul>
   * <li>$local.dir/usercache/$user</li>
   * <li>$local.dir/usercache/$user/appcache</li>
   * <li>$local.dir/usercache/$user/filecache</li>
   * </ul>
   *
   * @param localDirs the target directories to create
   * @param user the user whose local cache directories should be initialized
   * @throws IOException if there's an issue initializing the cache
   * directories
   */
  void createUserCacheDirs(List<String> localDirs, String user)
      throws IOException {
    LOG.info("Initializing user {}", user);

    boolean appcacheDirStatus = false;
    boolean distributedCacheDirStatus = false;
    FsPermission appCachePerms = new FsPermission(APPCACHE_PERM);
    FsPermission fileperms = new FsPermission(FILECACHE_PERM);

    for (String localDir : localDirs) {
      // create $local.dir/usercache/$user/appcache
      Path localDirPath = new Path(localDir);
      final Path appDir = getAppcacheDir(localDirPath, user);
      try {
        createDir(appDir, appCachePerms, true, user);
        appcacheDirStatus = true;
      } catch (IOException e) {
        LOG.warn("Unable to create app cache directory : {}", appDir, e);
      }
      // create $local.dir/usercache/$user/filecache
      final Path distDir = getFileCacheDir(localDirPath, user);
      try {
        createDir(distDir, fileperms, true, user);
        distributedCacheDirStatus = true;
      } catch (IOException e) {
        LOG.warn("Unable to create file cache directory : {}", distDir, e);
      }
    }
    if (!appcacheDirStatus) {
      throw new IOException("Not able to initialize app-cache directories "
          + "in any of the configured local directories for user " + user);
    }
    if (!distributedCacheDirStatus) {
      throw new IOException(
          "Not able to initialize distributed-cache directories "
              + "in any of the configured local directories for user "
              + user);
    }
  }

  /**
   * Initialize the local directories for a particular user.
   * <ul>
   * <li>$local.dir/usercache/$user/appcache/$appid</li>
   * </ul>
   *
   * @param localDirs the target directories to create
   * @param user the user whose local cache directories should be initialized
   * @param appId the application ID
   * @throws IOException if there's an issue initializing the application
   * directories
   */
  void createAppDirs(List<String> localDirs, String user, String appId)
      throws IOException {
    boolean initAppDirStatus = false;
    FsPermission appperms = new FsPermission(APPDIR_PERM);
    for (String localDir : localDirs) {
      Path fullAppDir = getApplicationDir(new Path(localDir), user, appId);
      // create $local.dir/usercache/$user/appcache/$appId
      try {
        createDir(fullAppDir, appperms, true, user);
        initAppDirStatus = true;
      } catch (IOException e) {
        LOG.warn("Unable to create app directory {}",
            fullAppDir, e);
      }
    }
    if (!initAppDirStatus) {
      throw new IOException("Not able to initialize app directories "
          + "in any of the configured local directories for app "
          + appId.toString());
    }
  }

  /**
   * Create application log directories on all disks.
   *
   * @param appId the application ID
   * @param logDirs the target directories to create
   * @param user the user whose local cache directories should be initialized
   * @throws IOException if there's an issue initializing the application log
   * directories
   */
  void createAppLogDirs(String appId, List<String> logDirs, String user)
      throws IOException {

    boolean appLogDirStatus = false;
    FsPermission appLogDirPerms = new
        FsPermission(getLogDirPermissions());
    for (String rootLogDir : logDirs) {
      // create $log.dir/$appid
      Path appLogDir = new Path(rootLogDir, appId);
      try {
        createDir(appLogDir, appLogDirPerms, true, user);
      } catch (IOException e) {
        LOG.warn("Unable to create the app-log directory : {}", appLogDir, e);
        continue;
      }
      appLogDirStatus = true;
    }
    if (!appLogDirStatus) {
      throw new IOException("Not able to initialize app-log directories "
          + "in any of the configured local directories for app " + appId);
    }
  }

  /**
   * Create application log directories on all disks.
   *
   * @param appId the application ID
   * @param containerId the container ID
   * @param logDirs the target directories to create
   * @param user the user as whom the directories should be created.
   * Used only on secure Windows hosts.
   * @throws IOException if there's an issue initializing the container log
   * directories
   */
  void createContainerLogDirs(String appId, String containerId,
      List<String> logDirs, String user) throws IOException {
    boolean containerLogDirStatus = false;
    FsPermission containerLogDirPerms = new
        FsPermission(getLogDirPermissions());
    for (String rootLogDir : logDirs) {
      // create $log.dir/$appid/$containerid
      Path appLogDir = new Path(rootLogDir, appId);
      Path containerLogDir = new Path(appLogDir, containerId);
      try {
        createDir(containerLogDir, containerLogDirPerms, true, user);
      } catch (IOException e) {
        LOG.warn("Unable to create the container-log directory : {}",
            appLogDir, e);
        continue;
      }
      containerLogDirStatus = true;
    }
    if (!containerLogDirStatus) {
      throw new IOException(
          "Not able to initialize container-log directories "
              + "in any of the configured local directories for container "
              + containerId);
    }
  }

  /**
   * Return the default container log directory permissions.
   *
   * @return the default container log directory permissions
   */
  @VisibleForTesting
  public String getLogDirPermissions() {
    if (this.logDirPermissions==null) {
      this.logDirPermissions = getConf().get(
          YarnConfiguration.NM_DEFAULT_CONTAINER_EXECUTOR_LOG_DIRS_PERMISSIONS,
          YarnConfiguration.NM_DEFAULT_CONTAINER_EXECUTOR_LOG_DIRS_PERMISSIONS_DEFAULT);
    }
    return this.logDirPermissions;
  }

  /**
   * Clear the internal variable for repeatable testing.
   */
  @VisibleForTesting
  public void clearLogDirPermissions() {
    this.logDirPermissions = null;
  }

  /**
   *
   * @param ctx Encapsulates information necessary for exec containers.
   * @return the input/output stream of interactive docker shell.
   * @throws ContainerExecutionException
   */
  @Override
  public IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException {
    return null;
  }

  /**
   * Return the list of paths of given local directories.
   *
   * @return the list of paths of given local directories
   */
  private static List<Path> getPaths(List<String> dirs) {
    List<Path> paths = new ArrayList<>(dirs.size());
    for (int i = 0; i < dirs.size(); i++) {
      paths.add(new Path(dirs.get(i)));
    }
    return paths;
  }

  @Override
  public void updateYarnSysFS(Context ctx, String user,
      String appId, String spec) throws IOException {
    throw new ServiceStateException("Implementation unavailable");
  }
}
