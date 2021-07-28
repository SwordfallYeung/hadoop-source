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

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.security.KerberosInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An inter-datanode protocol for updating generation stamp
 *
 * todo DataNode与DataNode之间的接口。InterDatanodeProtocol接口主要用于租约恢复操作
 *      客户端打开一个文件进行写操作时，首先要获取这个文件的租约，并且还需要定期更新租约。
 *      当NameNode的租约监控线程发现某个HDFS文件租约长期没有更新时，就会认为这个文件的
 *      客户端发送异常，这时NameNode就需要触发租约恢复操作——同步数据流管道中所有Datanode
 *      上该文件数据块的状态，并强制关闭这个文件。
 *
 * todo 租约恢复的控制并不是由NameNode负责的，而是NameNode从数据流管道中选出一个主恢复节点，
 *      然后通过下发DatanodeCommand的恢复指令触发这个数据节点控制租约恢复操作，也就是由这个
 *      主恢复节点协调整个租约恢复操作的过程。
 *
 * todo 租约恢复操作其实很简单，就是将数据流管道中所有数据节点上保存的同一个数据块状态(时间戳和数据块长度)
 *      同步一致。当成功完成租约恢复后，主恢复节点会调用DatanodeProtocol.commitBlock Synchronization()方法
 *      同步nameNode节点上该数据块的时间戳和数据块长度，保持名字节点和数据节点的一致。
 *
 * todo 由于数据流管道中同一个数据块状态(长度和时间戳)在不同的DataNode上可能是不一致的，
 *      所以主恢复节点会首先调用InterDatanodeProtocol.initReplicaRecovery()方法获取数据流管道中所有
 *      数据节点上保存的指定数据块的状态，这里的数据块状态使用ReplicaRecoveryInfo类封装。主恢复节点会
 *      根据收集到的这些状态，确定一个当前数据块的新长度，并且使用NameNode下发的recoverId作为数据块的时间戳。
 *
 * todo 当完成了所有的时间同步操作后，主恢复节点节就可以调用DatanodeProtocol.commitBlock Synchronization()
 *      将NameNode上该数据块的长度和时间戳同步为新的长度和时间戳，这样DataNode和NameNode的数据也就一致了。
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface InterDatanodeProtocol {
  Logger LOG = LoggerFactory.getLogger(InterDatanodeProtocol.class.getName());

  /**
   * Until version 9, this class InterDatanodeProtocol served as both
   * the interface to the DN AND the RPC protocol used to communicate with the 
   * DN.
   * 
   * This class is used by both the DN to insulate from the protocol 
   * serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in InterDatanodeProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   */
  public static final long versionID = 6L;

  /**
   * Initialize a replica recovery.
   * 
   * @return actual state of the replica on this data-node or 
   * null if data-node does not have the replica.
   *
   * todo 由于数据流管道中同一个数据块状态(长度和时间戳)在不同的Datanode上可能是不一致的，
   *      所以主恢复节点会首先调用InterDatanodeProtocol.initReplicaRecovery()方法
   *      获取数据流管道中所有数据节点上保存的指定数据块的状态，这里的数据块状态使用 ReplicaRecoveryInfo类封装。
   *      主恢复节点会根据收集到的这些状态，确定一个当前数据块的新长度，并且使用NameNode下发的recoverId作为数据块的新时间戳。
   */
  ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException;

  /**
   * Update replica with the new generation stamp and length.
   *
   * todo 将数据流管道中所有节点上该数 据块的长度同步为新的长度，将数据块的时间戳同步为新的时间戳。
   */
  String updateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId,
                                    long newBlockId, long newLength)
      throws IOException;
}
