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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.*;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

import javax.annotation.Nonnull;

/**********************************************************************
 * Protocol that a DFS datanode uses to communicate with the NameNode.
 * It's used to upload current load information and block reports.
 *
 * The only way a NameNode can communicate with a DataNode is by
 * returning values from these functions.
 *
 **********************************************************************/
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, 
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface DatanodeProtocol {
  /**
   * This class is used by both the Namenode (client) and BackupNode (server) 
   * to insulate from the protocol serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in DatanodeProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   */
  public static final long versionID = 28L;
  
  // error code
  final static int NOTIFY = 0;
  final static int DISK_ERROR = 1; // there are still valid volumes on DN
  final static int INVALID_BLOCK = 2;
  final static int FATAL_DISK_ERROR = 3; // no valid volumes left on DN

  /**
   * Determines actions that data node should perform 
   * when receiving a datanode command. 
   */
  // todo 未定义
  final static int DNA_UNKNOWN = 0;    // unknown action
  // todo 数据块复制
  //   DNA_TRANSFER指令用于触发数据节点的数据块复制操作
  //   当HDFS系统中某个数据块的副本数小于配置的副本系数时，
  //   NameNode会通过DNA_TRANSFER指令通知某个拥有这个数据块副本的DataNode将该数据块复制到其他数据节点上。
  final static int DNA_TRANSFER = 1;   // transfer blocks to another datanode
  /**
   * todo 数据块删除
   *    DNA_INVALIDATE用于通知DataNode删除数据节点上的指定数据块，
   *    这是因为NameNode发现了某个数据块的副本数已经超过了配置的副本系数，
   *    这时NameNode会通知某个数据节点删除这个数据节点上多余的数据块副本。
   */
  final static int DNA_INVALIDATE = 2; // invalidate blocks
  /**
   * todo 关闭数据节点
   *    DNA_SHUTDOWN已经废弃不用了，DataNode接收到DNA_SHUTDOWN指令后会
   *    直接抛出UnsupportedOperationException异常。关闭DataNode是通过
   *    调用ClientDatanodeProtocol.shutdownDatanode()方法来触发的。
   */
  final static int DNA_SHUTDOWN = 3;   // shutdown node
  // todo 重新注册数据节点
  final static int DNA_REGISTER = 4;   // re-register
  // todo 提交上一次升级
  final static int DNA_FINALIZE = 5;   // finalize previous upgrade
  /**
   * todo 数据块恢复
   *      当客户端在写文件时发生异常退出，会造成数据流管道中不同数据节点上数据状态的不一致，
   *      这时NameNode会从数据流管道中选出一个数据节点作为主恢复节点，协调数据流管道中的其他
   *      数据节点进行租约恢复操作，以同步这个数据块的状态。
   */
  final static int DNA_RECOVERBLOCK = 6;  // request a block recovery
  // todo 安全相关
  final static int DNA_ACCESSKEYUPDATE = 7;  // update access key
  // todo 更新平衡器宽度
  final static int DNA_BALANCERBANDWIDTHUPDATE = 8; // update balancer bandwidth
  // todo 缓存数据块
  final static int DNA_CACHE = 9;      // cache blocks
  // todo 取消缓存数据块
  final static int DNA_UNCACHE = 10;   // uncache blocks
  // todo 擦除编码重建命令
  final static int DNA_ERASURE_CODING_RECONSTRUCTION = 11; // erasure coding reconstruction command
  // todo 块存储移动命令
  int DNA_BLOCK_STORAGE_MOVEMENT = 12; // block storage movement command
  // todo 删除sps工作命令
  int DNA_DROP_SPS_WORK_COMMAND = 13; // drop sps work command

  /** 
   * Register Datanode.
   *
   * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem#registerDatanode(DatanodeRegistration)
   * @param registration datanode registration information
   * @return the given {@link org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration} with
   *  updated registration information
   *
   *  todo 一个完整的DataNode启动操作会与NameNode进行4次交互，也就是调用4次DatanodeProtocol定义的方法。
   *      首先调用versionRequest()与NameNode进行握手操作，
   *      然后调用registerDatanode()向NameNode注册当前的DataNode，
   *      接着调用blockReport()汇报DataNode上存储的所有数据块，
   *      最后调用cacheReport()汇报DataNode缓存的所有数据块。
   *
   * todo 成功进行握手操作后，Datanode会调用ClientProtocol.registerDatanode()方法向NameNode注册当前的DataNode，
   *      这个方法的参数是一个DatanodeRegistration对象，它封装了DatanodeID、DataNode的存储系统的布局版本号（layoutVersion）、
   *      当前命名空间的ID(namespaceId)、集群ID(clusterId)、文件系统的创建时间(ctime)以及DataNode当前的软件版本好(softwareVersion)。
   *
   * todo nameNode节点会判断DataNode的软件版本号与NameNode的软件版本号是否兼容,
   *      如果兼容则进行注册操作，并返回一个DatanodeRegistration对象供DataNode后续处理逻辑使用。
   */
  @Idempotent
  public DatanodeRegistration registerDatanode(DatanodeRegistration registration
      ) throws IOException;
  
  /**
   * sendHeartbeat() tells the NameNode that the DataNode is still
   * alive and well.  Includes some status info, too. 
   * It also gives the NameNode a chance to return 
   * an array of "DatanodeCommand" objects in HeartbeatResponse.
   * A DatanodeCommand tells the DataNode to invalidate local block(s), 
   * or to copy them to other DataNodes, etc.
   * @param registration datanode registration information
   * @param reports utilization report per storage
   * @param xmitsInProgress number of transfers from this datanode to others
   * @param xceiverCount number of active transceiver threads
   * @param failedVolumes number of failed volumes
   * @param volumeFailureSummary info about volume failures
   * @param requestFullBlockReportLease whether to request a full block
   *                                    report lease.
   * @param slowPeers Details of peer DataNodes that were detected as being
   *                  slow to respond to packet writes. Empty report if no
   *                  slow peers were detected by the DataNode.
   * @throws IOException on error
   *
   * todo DataNode会定期向NameNode发送心跳  dfs.heartbeat.interval配置项配置，默认是3秒
   *
   * todo 用于心跳汇报的接口，除了携带标识DataNode身份的DatanodeRegistration对象外，还包括
   *      数据节点上所有存储的状态、缓存的状态、正在写文件数据的连接数、读写数据使用的线程数等。
   *
   * todo sendHeartbeat()会返回一个HeartbeatResponse对象，这个对象包含了NameNode向DataNode发送的
   *      名字节点指令，以及当前NameNode的HA状态。
   *
   * todo 需要特别注意的是，在开启了HA的HDFS集群中，DataNode是需要同时向Active NameNode以及Standby NameNode
   *      发送心跳的，不过只有ActiveNameNode才能想DataNode下发节点指令。
   */
  @Idempotent
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
                                       StorageReport[] reports,
                                       long dnCacheCapacity,
                                       long dnCacheUsed,
                                       int xmitsInProgress,
                                       int xceiverCount,
                                       int failedVolumes,
                                       VolumeFailureSummary volumeFailureSummary,
                                       boolean requestFullBlockReportLease,
                                       @Nonnull SlowPeerReports slowPeers,
                                       @Nonnull SlowDiskReports slowDisks)
      throws IOException;

  /**
   * blockReport() tells the NameNode about all the locally-stored blocks.
   * The NameNode returns an array of Blocks that have become obsolete
   * and should be deleted.  This function is meant to upload *all*
   * the locally-stored blocks.  It's invoked upon startup and then
   * infrequently afterwards.
   * @param registration datanode registration
   * @param poolId the block pool ID for the blocks
   * @param reports report of blocks per storage
   *     Each finalized block is represented as 3 longs. Each under-
   *     construction replica is represented as 4 longs.
   *     This is done instead of Block[] to reduce memory used by block reports.
   * @param context Context information for this block report.
   *
   * @return - the next command for DN to process.
   * @throws IOException
   *
   * todo DataNode成功向NameNode注册之后，DataNode会通过调用DatanodeProtocol.blockReport()方法向NameNode上报
   *    它管理的所有数据块的信息。这个方法需要三个参数：
   *        DatanodeRegistration用于标识当前的DataNode；
   *        poolId用于标识数据块所在的块池ID；
   *        reports是一个StorageBlockReport对象的数组，每个StorageBlockReport对象都用于记录DataNode上一个存储空间存储的数据块。
   *
   * todo 这里需要特别注意的是，上报的数据块是以长整型数组保存的，每个已经提交的数据块(finalized)以3个长整型来表示，
   *      每个构建中的数据块(under-construction)以4个长整型来表示。之所以不使用ExtendedBlock对象保存上报的数据块，是因为这样可以减少
   *      blockReport()操作所使用的内存。
   *
   * todo NameNode接收到消息时，不需要创建大量的ExtendedBlock对象，只需要不断地从长整型数组中提取数据块即可。
   */
  @Idempotent
  public DatanodeCommand blockReport(DatanodeRegistration registration,
            String poolId, StorageBlockReport[] reports,
            BlockReportContext context) throws IOException;
    

  /**
   * Communicates the complete list of locally cached blocks to the NameNode.
   * 
   * This method is similar to
   * {@link #blockReport(DatanodeRegistration, String, StorageBlockReport[], BlockReportContext)},
   * which is used to communicated blocks stored on disk.
   *
   * @param registration The datanode registration.
   * @param poolId     The block pool ID for the blocks.
   * @param blockIds   A list of block IDs.
   * @return           The DatanodeCommand.
   * @throws IOException
   *
   * todo NameNode接收到blockReport()请求之后，会根据DataNode上报的数据块存储情况建立数据块与数据节点之间的对应关系。
   *      同时，NameNode会在blockReport()的响应中携带名字节点指令，通知数据节点进行重新注册、发送心跳、备份或者删除DataNode
   *      本地磁盘上数据块副本的操作。这些名字节点指令都是以DatanodeCommand对象封装的。
   *
   * todo blockReport()方法只在Datanode启动时以及指定间隔时执行一次。间隔是由 dfs.blockreport.intervalMsec 参数配置的，默认是6小时执行一次
   */
  @Idempotent
  public DatanodeCommand cacheReport(DatanodeRegistration registration,
      String poolId, List<Long> blockIds) throws IOException;

  /**
   * blockReceivedAndDeleted() allows the DataNode to tell the NameNode about
   * recently-received and -deleted block data. 
   * 
   * For the case of received blocks, a hint for preferred replica to be 
   * deleted when there is any excessive blocks is provided.
   * For example, whenever client code
   * writes a new Block here, or another DataNode copies a Block to
   * this DataNode, it will call blockReceived().
   *
   * todo DataNode会定期(默认是5分钟，不可以配置)调用blockReceivedAndDeleted()方法向NameNode
   *      汇报DataNode新接受的数据块或者删除的数据块。
   *
   * todo DataNode接受一个数据块，可能是因为Client写入了新的数据块，或者从别的DataNode上复制一个
   *      数据块到当前DataNode。
   *
   * todo DataNode删除一个数据块，则有可能是因为该数据块的副本数量过多，NameNode向当前DataNode下发
   *      了删除数据块副本的指令。
   *
   * todo 我们可以把blockReceivedAndDeleted()方法理解为blockReport()的增量汇报，这个方法的参数包括
   *      DatanodeRegistration对象、增量汇报数据块所在的块池ID以及StorageReceivedDeletedBlocks对象
   *      的数组，这里的StorageReceivedDeletedBlocks对象封装了DataNode的一个数据存储上新添加以及删除
   *      的数据块集合。
   *
   * todo NameNode接受了这个请求之后，会更新它内存中数据块与数据节点的对应关系。
   *
   */
  @Idempotent
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
                            String poolId,
                            StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks)
                            throws IOException;

  /**
   * errorReport() tells the NameNode about something that has gone
   * awry.  Useful for debugging.
   *
   * todo 该方法用于向名字节点上报运行过程中发生的一些状况，如磁盘不可用等。
   */
  @Idempotent
  public void errorReport(DatanodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException;

  /**
   * todo 这个方法的返回值是一个NamespaceInfo对象，Namespace对象会封装当前HDFS集群的命名空间信息，
   *      包括存储系统的布局版本号(layoutversion)、当前的命名空间的ID(namespaceId)、集群ID(clusterId)、
   *      文件系统的创建时间 (ctime)、构建时的HDFS版本号(buildVersion)、块池ID(blockpoolId)、当前的软件版本号(softwareVersion)等
   *
   * todo DataNode获取到NamespaceInfo对象后，就会比较DataNode前的HDFS版本号和NameNode的HDFS版本号，
   *      如果DataNode版本与NameNode版本不能协同工作，则抛出异常，DataNode也就无法注册到该NameNode上。
   *      如果当前DataNode上已经有了文件存储的目录，那么DataNode还会检查DataNode存储上的块池ID、文件
   *      系统ID以及集群ID与NameNode返回的是否一致。
   */
  @Idempotent
  public NamespaceInfo versionRequest() throws IOException;

  /**
   * same as {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#reportBadBlocks(LocatedBlock[])}
   * }
   *
   * todo reportBadBlocks()与ClientProtocol.reportBad.Blocks()方法很类似，DataNode会调用这个方法向NameNode
   *      汇报损坏的数据块。
   *
   * todo DataNode会在三种情况下调用这个方法：
   *      1. DataBlockScanner线程定期扫描数据节点上存储的数据块，发现数据块的校验出现错误时;
   *      2. 数据流管道写数据时，DataNode接受了一个新的数据块，进行数据块校验操作出现错误时;
   *      3. 进行数据块复制操作(DataTransfer)，DataNode读取本地存储的数据块时，发现本地数据块副本的长度小于
   *         NameNode记录的长度，则认为该数据块已经无效，会调用reportBadBlocks()方法。
   *
   * todo reportBadBlocks()方法的参数是LocatedBlock对象，这个对象描述了出现错误数据块的位置，NameNode收到reportBadBlocks()
   *      请求后，会下发数据副本删除指令删除错误的数据块。
   */
  @Idempotent
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;
  
  /**
   * Commit block synchronization in lease recovery
   *
   * todo 用于在租约恢复操作时同步数据块的状态。
   *      在租约恢复操作时，主数据节点完成所有租约恢复协调操作后调用 commitBlockSynchronization()方法
   *      同步Datanode和Namenode上数据块的状态，所以commitBlockSynchronization()方法包含了大量的参数
   */
  @Idempotent
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
      String[] newtargetstorages) throws IOException;
}
