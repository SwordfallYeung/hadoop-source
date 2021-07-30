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

package org.apache.hadoop.ipc;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.SocketFactory;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolInfoService;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 *
 * todo RPC(Remote Procedure Call)即远程过程调用，是一种通过网络从远程计算机程序上请求服务的协议。RPC允许本地程序像调用本地方法
 *      一样调用远程计算机上的应用程序，其使用常见的网络传输协议(如TCP或UDP)传递RPC请求以及相应信息，使得分布式程序的开发更加容易。
 *      Hadoop作为分布式存储系统，各个节点之间的通信和交互是必不可少的，所以需要实现一套节点间的通信交互机制。
 *
 * todo Hadoop实现了一套自己的RPC框架。采用了JavaNIO、Java动态代理以及protobuf等基础技术
 *      RPC采用客户端/服务器模式，请求程序就是一个客户端，而服务提供程序就是一个服务器。客户端首先会发送一个有参数的调用请求到服务器，
 *      然后等待服务器发回响应信息。在服务器端，服务提供程序会保持睡眠状态直到有调用请求到达为止。当一个调用请求到达后，服务提供程序
 *      会执行调用请求，计算结果，向客户端发送响应信息，然后等待下一个调用请求。最后，客户端成功地接收服务器发回的响应信息，一个远程调用结束。
 *
 * todo        client functions                              server functions
 *  ① rpc call  .|  |` ⑩ rpc return                 ⑤ local call `|  |. ⑥ local return
 *            client sub                                       server stub
 *      ② send  .|  |` ⑨ receive   ③ network          ④ receive  `| |. ⑦ send
 *              sockets        ——————————————————>              sockets
 *                             <——————————————————
 *                                 ⑧ network
 *  RPC框架工作原理示例图如上：
 *  1. client functions：请求程序，会像调用本地方法一样调用客户端stub程序（图1），然后接收stub程序的响应信息（图10）；
 *  2. client stub：客户端stub程序，表现得就像本地程序一样，但底层却会调用请求和参数序列化并通过通信模块发送给服务器（图2）；
 *  3. sockets：网络通信模块，用于传输RPC请求和响应（图3和8），可以基于TCP或UDP协议；
 *  4. server stub：服务端stub程序，会接收客户端发送的请求和参数（图4）并发序列化，根据调用信息触发对应的服务程序（图5），
 *     然后将服务程序的响应信息（图6），序列化并发回给客户端（图7）；
 *  5. server functions：服务程序，会接收服务端stub程序的调用请求（图5），执行对应的逻辑并返回执行结果（图6）。
 *
 * todo Hadoop RPC实现
 *      HadoopRPC实现方式跟上图一样，代码位于hadoop-common中的org.apache.hadoop.ipc包下。
 *      Hadoop RPC框架主要由三个类组成：RPC、Client和Server类。
 *          1. RPC类用于对外提供一个使用Hadoop RPC框架的接口；
 *          2. Client类用于实现RPC客户端功能；
 *          3. Server类则用于实现RPC服务器端功能。
 *
 *  todo 在test编写demo，采用的是hadoop默认的RPC Engine: WritableRpcEngine，采用proto协议。
 *
 */
@InterfaceAudience.LimitedPrivate(value = { "Common", "HDFS", "MapReduce", "Yarn" })
@InterfaceStability.Evolving
public class RPC {
  final static int RPC_SERVICE_CLASS_DEFAULT = 0;
  public enum RpcKind {
    RPC_BUILTIN ((short) 1),         // Used for built in calls by tests
    RPC_WRITABLE ((short) 2),        // Use WritableRpcEngine 
    RPC_PROTOCOL_BUFFER ((short) 3); // Use ProtobufRpcEngine
    final static short MAX_INDEX = RPC_PROTOCOL_BUFFER.value; // used for array size
    private final short value;

    RpcKind(short val) {
      this.value = val;
    } 
  }
  
  interface RpcInvoker {   
    /**
     * Process a client call on the server side
     * @param server the server within whose context this rpc call is made
     * @param protocol - the protocol name (the class of the client proxy
     *      used to make calls to the rpc server.
     * @param rpcRequest  - deserialized
     * @param receiveTime time at which the call received (for metrics)
     * @return the call's return
     * @throws IOException
     **/
    public Writable call(Server server, String protocol,
        Writable rpcRequest, long receiveTime) throws Exception ;
  }
  
  static final Logger LOG = LoggerFactory.getLogger(RPC.class);
  
  /**
   * Get all superInterfaces that extend VersionedProtocol
   * @param childInterfaces
   * @return the super interfaces that extend VersionedProtocol
   */
  static Class<?>[] getSuperInterfaces(Class<?>[] childInterfaces) {
    List<Class<?>> allInterfaces = new ArrayList<Class<?>>();

    for (Class<?> childInterface : childInterfaces) {
      if (VersionedProtocol.class.isAssignableFrom(childInterface)) {
          allInterfaces.add(childInterface);
          allInterfaces.addAll(
              Arrays.asList(
                  getSuperInterfaces(childInterface.getInterfaces())));
      } else {
        LOG.warn("Interface " + childInterface +
              " ignored because it does not extend VersionedProtocol");
      }
    }
    return allInterfaces.toArray(new Class[allInterfaces.size()]);
  }
  
  /**
   * Get all interfaces that the given protocol implements or extends
   * which are assignable from VersionedProtocol.
   */
  static Class<?>[] getProtocolInterfaces(Class<?> protocol) {
    Class<?>[] interfaces  = protocol.getInterfaces();
    return getSuperInterfaces(interfaces);
  }
  
  /**
   * Get the protocol name.
   *  If the protocol class has a ProtocolAnnotation, then get the protocol
   *  name from the annotation; otherwise the class name is the protocol name.
   */
  static public String getProtocolName(Class<?> protocol) {
    if (protocol == null) {
      return null;
    }
    ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
    return  (anno == null) ? protocol.getName() : anno.protocolName();
  }
  
  /**
   * Get the protocol version from protocol class.
   * If the protocol class has a ProtocolAnnotation,
   * then get the protocol version from the annotation;
   * otherwise get it from the versionID field of the protocol class.
   */
  static public long getProtocolVersion(Class<?> protocol) {
    if (protocol == null) {
      throw new IllegalArgumentException("Null protocol");
    }
    long version;
    ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
    if (anno != null) {
      version = anno.protocolVersion();
      if (version != -1)
        return version;
    }
    try {
      Field versionField = protocol.getField("versionID");
      versionField.setAccessible(true);
      return versionField.getLong(protocol);
    } catch (NoSuchFieldException ex) {
      throw new RuntimeException(ex);
    } catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
  }

  private RPC() {}                                  // no public ctor

  // cache of RpcEngines by protocol
  private static final Map<Class<?>,RpcEngine> PROTOCOL_ENGINES
    = new HashMap<Class<?>,RpcEngine>();

  private static final String ENGINE_PROP = "rpc.engine";

  /**
   * Set a protocol to use a non-default RpcEngine if one
   * is not specified in the configuration.
   * @param conf configuration to use
   * @param protocol the protocol interface
   * @param engine the RpcEngine impl
   */
  public static void setProtocolEngine(Configuration conf,
                                Class<?> protocol, Class<?> engine) {
    if (conf.get(ENGINE_PROP+"."+protocol.getName()) == null) {
      conf.setClass(ENGINE_PROP+"."+protocol.getName(), engine,
                    RpcEngine.class);
    }
  }

  // return the RpcEngine configured to handle a protocol
  // todo 最主要的是getProtocolEngine方法，获取RpcEngine，这个方法加了synchronized关键字，所以是个同步方法。
  static synchronized RpcEngine getProtocolEngine(Class<?> protocol,
      Configuration conf) {
    // todo 从缓存中获取RpcEngine
    // todo 这个是提前设置的，通过RPC.setProtocolEngine(conf, MetaInfoProtocol.class,ProtobufRpcEngine.class);
    // todo RpcEngine有三种ProtobufRpcEngine(已过时)、ProtobufRpcEngine2和WritableRpcEngine(已过时)，默认是WritableRpcEngine，下述代码会走默认的WritableRpcEngine
    // todo ProtobufRpcEngine、ProtobufRpcEngine2和WritableRpcEngine这三个类型都会调用RPC.Server父类的初始化，然后RPC.Server最终调用父类ipc.Server的初始化
    RpcEngine engine = PROTOCOL_ENGINES.get(protocol);
    if (engine == null) {
      // todo 通过这里，获取RpcEngine的实现类，这里我们获取的是 ProtobufRpcEngine2.class
      Class<?> impl = conf.getClass(ENGINE_PROP+"."+protocol.getName(),
                                    WritableRpcEngine.class);
      // todo impl: org.apache.hadoop.ipc.ProtobufRpcEngine2
      engine = (RpcEngine)ReflectionUtils.newInstance(impl, conf);
      PROTOCOL_ENGINES.put(protocol, engine);
    }
    return engine;
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  public static class VersionMismatch extends RpcServerException {
    private static final long serialVersionUID = 0;

    private String interfaceName;
    private long clientVersion;
    private long serverVersion;
    
    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }
    
    /**
     * Get the interface name
     * @return the java class name 
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }
    
    /**
     * Get the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }
    
    /**
     * Get the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
    /**
     * get the rpc status corresponding to this exception
     */
    public RpcStatusProto getRpcStatusProto() {
      return RpcStatusProto.ERROR;
    }

    /**
     * get the detailed rpc status corresponding to this exception
     */
    public RpcErrorCodeProto getRpcErrorCodeProto() {
      return RpcErrorCodeProto.ERROR_RPC_VERSION_MISMATCH;
    }
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(
      Class<T> protocol,
      long clientVersion,
      InetSocketAddress addr,
      Configuration conf
      ) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr,
                             Configuration conf) throws IOException {
    return waitForProtocolProxy(
        protocol, clientVersion, addr, conf, Long.MAX_VALUE);
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(Class<T> protocol, long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             long connTimeout) throws IOException { 
    return waitForProtocolProxy(protocol, clientVersion, addr,
        conf, connTimeout).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             long connTimeout) throws IOException { 
    return waitForProtocolProxy(protocol, clientVersion, addr, conf,
        getRpcTimeout(conf), null, connTimeout);
  }
  
  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             int rpcTimeout,
                             long timeout) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr,
        conf, rpcTimeout, null, timeout).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                               long clientVersion,
                               InetSocketAddress addr, Configuration conf,
                               int rpcTimeout,
                               RetryPolicy connectionRetryPolicy,
                               long timeout) throws IOException { 
    long startTime = Time.now();
    IOException ioe;
    while (true) {
      try {
        return getProtocolProxy(protocol, clientVersion, addr, 
            UserGroupInformation.getCurrentUser(), conf, NetUtils
            .getDefaultSocketFactory(conf), rpcTimeout, connectionRetryPolicy);
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
        ioe = se;
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      } catch(NoRouteToHostException nrthe) { // perhaps a VIP is failing over
        LOG.info("No route to host for server: " + addr);
        ioe = nrthe;
      }
      // check if timed out
      if (Time.now()-timeout >= startTime) {
        throw ioe;
      }

      if (Thread.currentThread().isInterrupted()) {
        // interrupted during some IO; this may not have been caught
        throw new InterruptedIOException("Interrupted waiting for the proxy");
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw (IOException) new InterruptedIOException(
            "Interrupted waiting for the proxy").initCause(ioe);
      }
    }
  }

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, conf, factory).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param factory socket factory
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf,
                                SocketFactory factory) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return getProtocolProxy(protocol, clientVersion, addr, ugi, conf, factory);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, ticket, conf, factory).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param ticket user group information
   * @param conf configuration to use
   * @param factory socket factory
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(protocol, clientVersion, addr, ticket, conf,
        factory, getRpcTimeout(conf), null);
  }
  
  /**
   * Construct a client-side proxy that implements the named protocol,
   * talking to a server at the named address.
   * @param <T>
   * 
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout) throws IOException {
    return getProtocolProxy(protocol, clientVersion, addr, ticket,
             conf, factory, rpcTimeout, null).getProxy();
  }
  
  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @param connectionRetryPolicy retry policy
   * @return the proxy
   * @throws IOException if any error occurs
   */
   public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout,
                                RetryPolicy connectionRetryPolicy) throws IOException {    
     return getProtocolProxy(protocol, clientVersion, addr, ticket,
       conf, factory, rpcTimeout, connectionRetryPolicy, null);
   }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   *
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @param connectionRetryPolicy retry policy
   * @param fallbackToSimpleAuth set to true or false during calls to indicate if
   *   a secure client falls back to simple auth
   * @return the proxy
   * @throws IOException if any error occurs
   *
   * todo 获取代理对象
   */
   public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout,
                                RetryPolicy connectionRetryPolicy,
                                AtomicBoolean fallbackToSimpleAuth)
       throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    // todo 先通过getProtocolEngine(protocol, conf)这个方法，获取到RpcEngine，然后调用getProxy获取对应的对象
    //      这个是getProxy的接口定义，根据RpcEngine的不同，实现方式也不同
    //      拿到代理对象之后，就可以像本地一样调用里面的方法了。
    return getProtocolEngine(protocol, conf).getProxy(protocol, clientVersion,
        addr, ticket, conf, factory, rpcTimeout, connectionRetryPolicy,
        fallbackToSimpleAuth, null);
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server.
   *
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @param connectionRetryPolicy retry policy
   * @param fallbackToSimpleAuth set to true or false during calls to indicate
   *   if a secure client falls back to simple auth
   * @param alignmentContext state alignment context
   * @return the proxy
   * @throws IOException if any error occurs
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout,
                                RetryPolicy connectionRetryPolicy,
                                AtomicBoolean fallbackToSimpleAuth,
                                AlignmentContext alignmentContext)
       throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    return getProtocolEngine(protocol, conf).getProxy(protocol, clientVersion,
        addr, ticket, conf, factory, rpcTimeout, connectionRetryPolicy,
        fallbackToSimpleAuth, alignmentContext);
  }

   /**
    * Construct a client-side proxy object with the default SocketFactory
    * @param <T>
    * 
    * @param protocol
    * @param clientVersion
    * @param addr
    * @param conf
    * @return a proxy instance
    * @throws IOException
    *
    * todo 获取代理对象
    */
   public static <T> T getProxy(Class<T> protocol,
                                 long clientVersion,
                                 InetSocketAddress addr, Configuration conf)
     throws IOException {

     return getProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
   }
  
  /**
   * Returns the server address for a given proxy.
   */
  public static InetSocketAddress getServerAddress(Object proxy) {
    return getConnectionIdForProxy(proxy).getAddress();
  }

  /**
   * Return the connection ID of the given object. If the provided object is in
   * fact a protocol translator, we'll get the connection ID of the underlying
   * proxy object.
   * 
   * @param proxy the proxy object to get the connection ID of.
   * @return the connection ID for the provided proxy object.
   */
  public static ConnectionId getConnectionIdForProxy(Object proxy) {
    if (proxy instanceof ProtocolTranslator) {
      proxy = ((ProtocolTranslator)proxy).getUnderlyingProxyObject();
    }
    RpcInvocationHandler inv = (RpcInvocationHandler) Proxy
        .getInvocationHandler(proxy);
    return inv.getConnectionId();
  }
   
  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a protocol proxy
   * @throws IOException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf)
    throws IOException {

    return getProtocolProxy(protocol, clientVersion, addr, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }

  /**
   * Stop the proxy. Proxy must either implement {@link Closeable} or must have
   * associated {@link RpcInvocationHandler}.
   * 
   * @param proxy
   *          the RPC proxy object to be stopped
   * @throws HadoopIllegalArgumentException
   *           if the proxy does not implement {@link Closeable} interface or
   *           does not have closeable {@link InvocationHandler}
   */
  public static void stopProxy(Object proxy) {
    if (proxy == null) {
      throw new HadoopIllegalArgumentException(
          "Cannot close proxy since it is null");
    }
    try {
      if (proxy instanceof Closeable) {
        ((Closeable) proxy).close();
        return;
      } else {
        InvocationHandler handler = Proxy.getInvocationHandler(proxy);
        if (handler instanceof Closeable) {
          ((Closeable) handler).close();
          return;
        }
      }
    } catch (IOException e) {
      LOG.error("Closing proxy or invocation handler caused exception", e);
    } catch (IllegalArgumentException e) {
      LOG.error("RPC.stopProxy called on non proxy: class=" + proxy.getClass().getName(), e);
    }
    
    // If you see this error on a mock object in a unit test you're
    // developing, make sure to use MockitoUtil.mockProtocol() to
    // create your mock.
    throw new HadoopIllegalArgumentException(
        "Cannot close proxy - is not Closeable or "
            + "does not provide closeable invocation handler "
            + proxy.getClass());
  }
  /**
   * Get the RPC time from configuration;
   * If not set in the configuration, return the default value.
   *
   * @param conf Configuration
   * @return the RPC timeout (ms)
   */
  public static int getRpcTimeout(Configuration conf) {
    return conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);
  }

  /**
   * Class to construct instances of RPC server with specific options.
   *
   * todo 其实就是通过RPC.Builder构建一个Server对象
   *      Builder构建对象中包含构建Server的各种属性，如IP，端口，协议，实现类等等。一个builder只能绑定一个协议和实现类。
   *      当Builder中的各种属性填充完，满足构建Server的条件之后，就会构建Server对象，并且调用Server的start方法启动Server.
   */
  public static class Builder {
    // todo 设置协议
    private Class<?> protocol = null;
    // todo 设置协议的实例
    private Object instance = null;
    // todo 设置绑定地址
    private String bindAddress = "0.0.0.0";
    // todo 设置端口
    private int port = 0;
    // todo 这是处理任务的handler数量
    private int numHandlers = 1;
    // todo 设置读取任务的reader数量
    private int numReaders = -1;
    private int queueSizePerHandler = -1;
    private boolean verbose = false;
    private final Configuration conf;    
    private SecretManager<? extends TokenIdentifier> secretManager = null;
    private String portRangeConfig = null;
    private AlignmentContext alignmentContext = null;
    
    public Builder(Configuration conf) {
      this.conf = conf;
    }

    /** Mandatory field */
    public Builder setProtocol(Class<?> protocol) {
      this.protocol = protocol;
      return this;
    }
    
    /** Mandatory field */
    public Builder setInstance(Object instance) {
      this.instance = instance;
      return this;
    }
    
    /** Default: 0.0.0.0 */
    public Builder setBindAddress(String bindAddress) {
      this.bindAddress = bindAddress;
      return this;
    }
    
    /** Default: 0 */
    public Builder setPort(int port) {
      this.port = port;
      return this;
    }
    
    /** Default: 1 */
    public Builder setNumHandlers(int numHandlers) {
      this.numHandlers = numHandlers;
      return this;
    }
    
    /** Default: -1 */
    public Builder setnumReaders(int numReaders) {
      this.numReaders = numReaders;
      return this;
    }
    
    /** Default: -1 */
    public Builder setQueueSizePerHandler(int queueSizePerHandler) {
      this.queueSizePerHandler = queueSizePerHandler;
      return this;
    }
    
    /** Default: false */
    public Builder setVerbose(boolean verbose) {
      this.verbose = verbose;
      return this;
    }
    
    /** Default: null */
    public Builder setSecretManager(
        SecretManager<? extends TokenIdentifier> secretManager) {
      this.secretManager = secretManager;
      return this;
    }
    
    /** Default: null */
    public Builder setPortRangeConfig(String portRangeConfig) {
      this.portRangeConfig = portRangeConfig;
      return this;
    }
    
    /** Default: null */
    public Builder setAlignmentContext(AlignmentContext alignmentContext) {
      this.alignmentContext = alignmentContext;
      return this;
    }

    /**
     * Build the RPC Server. 
     * @throws IOException on error
     * @throws HadoopIllegalArgumentException when mandatory fields are not set
     *
     * todo 最核心的是创建Server服务
     *      RPC.Server server = builder.build();
     *      在这里，主要的是有两个方法，一个是getProtocolEngine，另一个是getServer
     *      逻辑顺序是先获取对应协议的RpcEngine，然后再用RpcEngine创建一个Server服务。
     */
    public Server build() throws IOException, HadoopIllegalArgumentException {
      if (this.conf == null) {
        throw new HadoopIllegalArgumentException("conf is not set");
      }
      if (this.protocol == null) {
        throw new HadoopIllegalArgumentException("protocol is not set");
      }
      if (this.instance == null) {
        throw new HadoopIllegalArgumentException("instance is not set");
      }

      /**
       * todo 调用getProtocolEngine()获取当前RPC类配置的RpcEngine对象
       *      在NameNodeRpcServer的构建方法中已经将当前RPC类的RpcEngine对象设置为ProtobufRpcEngine了。
       *      获取ProtobufRpcEngine对象之后，build()方法会在ProtobufRpcEngine对象上调用getServer()方法获取一个RPC Server对象的引用。
       */
      return getProtocolEngine(this.protocol, this.conf).getServer(
          this.protocol, this.instance, this.bindAddress, this.port,
          this.numHandlers, this.numReaders, this.queueSizePerHandler,
          this.verbose, this.conf, this.secretManager, this.portRangeConfig,
          this.alignmentContext);
    }
  }
  
  /** An RPC Server. */
  public abstract static class Server extends org.apache.hadoop.ipc.Server {

    boolean verbose;

    private static final Pattern COMPLEX_SERVER_NAME_PATTERN =
        Pattern.compile("(?:[^\\$]*\\$)*([A-Za-z][^\\$]+)(?:\\$\\d+)?");

    /**
     * Get a meaningful and short name for a server based on a java class.
     *
     * The rules are defined to support the current naming schema of the
     * generated protobuf classes where the final class usually an anonymous
     * inner class of an inner class.
     *
     * 1. For simple classes it returns with the simple name of the classes
     *     (with the name without package name)
     *
     * 2. For inner classes, this is the simple name of the inner class.
     *
     * 3.  If it is an Object created from a class factory
     *   E.g., org.apache.hadoop.ipc.TestRPC$TestClass$2
     * this method returns parent class TestClass.
     *
     * 4. If it is an anonymous class E.g., 'org.apache.hadoop.ipc.TestRPC$10'
     * serverNameFromClass returns parent class TestRPC.
     *
     *
     */
    static String serverNameFromClass(Class<?> clazz) {
      String name = clazz.getName();
      String[] names = clazz.getName().split("\\.", -1);
      if (names != null && names.length > 0) {
        name = names[names.length - 1];
      }
      Matcher matcher = COMPLEX_SERVER_NAME_PATTERN.matcher(name);
      if (matcher.find()) {
        return matcher.group(1);
      } else {
        return name;
      }
    }
   
   /**
    * Store a map of protocol and version to its implementation
    */
   /**
    *  The key in Map
    */
   static class ProtoNameVer {
     final String protocol;
     final long   version;
     ProtoNameVer(String protocol, long ver) {
       this.protocol = protocol;
       this.version = ver;
     }
     @Override
     public boolean equals(Object o) {
       if (o == null) 
         return false;
       if (this == o) 
         return true;
       if (! (o instanceof ProtoNameVer))
         return false;
       ProtoNameVer pv = (ProtoNameVer) o;
       return ((pv.protocol.equals(this.protocol)) && 
           (pv.version == this.version));     
     }
     @Override
     public int hashCode() {
       return protocol.hashCode() * 37 + (int) version;    
     }
   }
   
   /**
    * The value in map
    */
   static class ProtoClassProtoImpl {
     final Class<?> protocolClass;
      final Object protocolImpl;
      private final boolean shadedPBImpl;

     ProtoClassProtoImpl(Class<?> protocolClass, Object protocolImpl) {
       this.protocolClass = protocolClass;
       this.protocolImpl = protocolImpl;
       this.shadedPBImpl = protocolImpl instanceof BlockingService;
     }

      public boolean isShadedPBImpl() {
        return shadedPBImpl;
      }
   }

   ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>> protocolImplMapArray = 
       new ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>>(RpcKind.MAX_INDEX);
   
   Map<ProtoNameVer, ProtoClassProtoImpl> getProtocolImplMap(RPC.RpcKind rpcKind) {
     if (protocolImplMapArray.size() == 0) {// initialize for all rpc kinds
       for (int i=0; i <= RpcKind.MAX_INDEX; ++i) {
         protocolImplMapArray.add(
             new HashMap<ProtoNameVer, ProtoClassProtoImpl>(10));
       }
     }
     return protocolImplMapArray.get(rpcKind.ordinal());   
   }
   
   // Register  protocol and its impl for rpc calls
   void registerProtocolAndImpl(RpcKind rpcKind, Class<?> protocolClass, 
       Object protocolImpl) {
     String protocolName = RPC.getProtocolName(protocolClass);
     long version;
     

     try {
       version = RPC.getProtocolVersion(protocolClass);
     } catch (Exception ex) {
       LOG.warn("Protocol "  + protocolClass + 
            " NOT registered as cannot get protocol version ");
       return;
     }


     getProtocolImplMap(rpcKind).put(new ProtoNameVer(protocolName, version),
         new ProtoClassProtoImpl(protocolClass, protocolImpl)); 
     if (LOG.isDebugEnabled()) {
       LOG.debug("RpcKind = " + rpcKind + " Protocol Name = " + protocolName +
           " version=" + version +
           " ProtocolImpl=" + protocolImpl.getClass().getName() +
           " protocolClass=" + protocolClass.getName());
     }
      String client = SecurityUtil.getClientPrincipal(protocolClass, getConf());
      if (client != null) {
        // notify the server's rpc scheduler that the protocol user has
        // highest priority.  the scheduler should exempt the user from
        // priority calculations.
        try {
          setPriorityLevel(UserGroupInformation.createRemoteUser(client), -1);
        } catch (Exception ex) {
          LOG.warn("Failed to set scheduling priority for " + client, ex);
        }
      }
    }
   
   static class VerProtocolImpl {
     final long version;
     final ProtoClassProtoImpl protocolTarget;
     VerProtocolImpl(long ver, ProtoClassProtoImpl protocolTarget) {
       this.version = ver;
       this.protocolTarget = protocolTarget;
     }
   }
   
   VerProtocolImpl[] getSupportedProtocolVersions(RPC.RpcKind rpcKind,
       String protocolName) {
     VerProtocolImpl[] resultk = 
         new  VerProtocolImpl[getProtocolImplMap(rpcKind).size()];
     int i = 0;
     for (Map.Entry<ProtoNameVer, ProtoClassProtoImpl> pv :
                                       getProtocolImplMap(rpcKind).entrySet()) {
       if (pv.getKey().protocol.equals(protocolName)) {
         resultk[i++] = 
             new VerProtocolImpl(pv.getKey().version, pv.getValue());
       }
     }
     if (i == 0) {
       return null;
     }
     VerProtocolImpl[] result = new VerProtocolImpl[i];
     System.arraycopy(resultk, 0, result, 0, i);
     return result;
   }
   
   VerProtocolImpl getHighestSupportedProtocol(RpcKind rpcKind, 
       String protocolName) {    
     Long highestVersion = 0L;
     ProtoClassProtoImpl highest = null;
     if (LOG.isDebugEnabled()) {
       LOG.debug("Size of protoMap for " + rpcKind + " ="
           + getProtocolImplMap(rpcKind).size());
     }
     for (Map.Entry<ProtoNameVer, ProtoClassProtoImpl> pv : 
           getProtocolImplMap(rpcKind).entrySet()) {
       if (pv.getKey().protocol.equals(protocolName)) {
         if ((highest == null) || (pv.getKey().version > highestVersion)) {
           highest = pv.getValue();
           highestVersion = pv.getKey().version;
         } 
       }
     }
     if (highest == null) {
       return null;
     }
     return new VerProtocolImpl(highestVersion,  highest);   
   }
   
    protected Server(String bindAddress, int port, 
                     Class<? extends Writable> paramClass, int handlerCount,
                     int numReaders, int queueSizePerHandler,
                     Configuration conf, String serverName, 
                     SecretManager<? extends TokenIdentifier> secretManager,
                     String portRangeConfig) throws IOException {
      super(bindAddress, port, paramClass, handlerCount, numReaders, queueSizePerHandler,
            conf, serverName, secretManager, portRangeConfig);
      initProtocolMetaInfo(conf);
    }
    
    private void initProtocolMetaInfo(Configuration conf) {
      RPC.setProtocolEngine(conf, ProtocolMetaInfoPB.class,
          ProtobufRpcEngine2.class);
      ProtocolMetaInfoServerSideTranslatorPB xlator = 
          new ProtocolMetaInfoServerSideTranslatorPB(this);
      BlockingService protocolInfoBlockingService = ProtocolInfoService
          .newReflectiveBlockingService(xlator);
      addProtocol(RpcKind.RPC_PROTOCOL_BUFFER, ProtocolMetaInfoPB.class,
          protocolInfoBlockingService);
    }
    
    /**
     * Add a protocol to the existing server.
     * @param protocolClass - the protocol class
     * @param protocolImpl - the impl of the protocol that will be called
     * @return the server (for convenience)
     */
    public Server addProtocol(RpcKind rpcKind, Class<?> protocolClass,
        Object protocolImpl) {
      registerProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
      return this;
    }
    
    @Override
    public Writable call(RPC.RpcKind rpcKind, String protocol,
        Writable rpcRequest, long receiveTime) throws Exception {
      return getServerRpcInvoker(rpcKind).call(this, protocol, rpcRequest,
          receiveTime);
    }
  }
}
