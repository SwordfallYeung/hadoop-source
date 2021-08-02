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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngine2Protos.RequestHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RPC Engine for for protobuf based RPCs.
 *
 * todo ProtobufRpcEngine目前是作为Hadoop RPC引擎唯一的实现方式。WritableRpcEngine在3.2.1版本已经被废弃，
 *      但依旧是默认实现的RPC引擎。ProtobufRpcEngine2过程比WriteRpcEngine的使用要麻烦很多，主要是在接口的定义
 *      和实现的手法上有区别
 *
 * todo 用法：
 *      1. 定义proto协议：根据Protocol的语法，定义一个服务MetaInfo，通过该服务的getMetaInfo接口，可以获取到元数据的信息。
 *      2. 根据定义好的proto协议生成java类，导入项目：将定义好的协议保存成文件，命名为CustomProtocol.proto。在该文件的目录下执行命令。
 *         这是在该目录下，会有一个文件目录生成，层级为我们定义好的org.apache.hadoop.rpc.protobuf路径。将里面生成的java文件CustomProtos.java
 *         导入到项目中，存放路径与定义好的路径一样。
 *      3. 创建一个接口，集成生成Java类中的CustomProtos.MetaInfo.BlockingInterface接口。
 *      4. 创建一个类，实现刚刚定义好的MetaInfoProtocol接口，并且实现里面的方法，这个需要自己去写代码逻辑。
 */
@InterfaceStability.Evolving
public class ProtobufRpcEngine2 implements RpcEngine {
  public static final Logger LOG =
      LoggerFactory.getLogger(ProtobufRpcEngine2.class);
  private static final ThreadLocal<AsyncGet<Message, Exception>>
      ASYNC_RETURN_MESSAGE = new ThreadLocal<>();

  static { // Register the rpcRequest deserializer for ProtobufRpcEngine
    registerProtocolEngine();
  }

  static void registerProtocolEngine() {
    if (Server.getRpcInvoker(RPC.RpcKind.RPC_PROTOCOL_BUFFER) == null) {
      org.apache.hadoop.ipc.Server
          .registerProtocolEngine(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
              ProtobufRpcEngine2.RpcProtobufRequest.class,
              new Server.ProtoBufRpcInvoker());
    }
  }

  private static final ClientCache CLIENTS = new ClientCache();

  @Unstable
  public static AsyncGet<Message, Exception> getAsyncReturnMessage() {
    return ASYNC_RETURN_MESSAGE.get();
  }

  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
        rpcTimeout, null);
  }

  @Override
  public <T> ProtocolProxy<T> getProxy(
      Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy)
      throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
      rpcTimeout, connectionRetryPolicy, null, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy,
      AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
      throws IOException {

    final Invoker invoker = new Invoker(protocol, addr, ticket, conf, factory,
        rpcTimeout, connectionRetryPolicy, fallbackToSimpleAuth,
        alignmentContext);
    return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[]{protocol}, invoker), false);
  }

  @Override
  public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException {
    Class<ProtocolMetaInfoPB> protocol = ProtocolMetaInfoPB.class;
    return new ProtocolProxy<ProtocolMetaInfoPB>(protocol,
        (ProtocolMetaInfoPB) Proxy.newProxyInstance(protocol.getClassLoader(),
            new Class[]{protocol}, new Invoker(protocol, connId, conf,
                factory)), false);
  }

  protected static class Invoker implements RpcInvocationHandler {
    private final Map<String, Message> returnTypes =
        new ConcurrentHashMap<String, Message>();
    private boolean isClosed = false;
    private final Client.ConnectionId remoteId;
    private final Client client;
    private final long clientProtocolVersion;
    private final String protocolName;
    private AtomicBoolean fallbackToSimpleAuth;
    private AlignmentContext alignmentContext;

    protected Invoker(Class<?> protocol, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
        throws IOException {
      this(protocol, Client.ConnectionId.getConnectionId(
          addr, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf),
          conf, factory);
      this.fallbackToSimpleAuth = fallbackToSimpleAuth;
      this.alignmentContext = alignmentContext;
    }

    /**
     * This constructor takes a connectionId, instead of creating a new one.
     */
    protected Invoker(Class<?> protocol, Client.ConnectionId connId,
        Configuration conf, SocketFactory factory) {
      this.remoteId = connId;
      this.client = CLIENTS.getClient(conf, factory, RpcWritable.Buffer.class);
      this.protocolName = RPC.getProtocolName(protocol);
      this.clientProtocolVersion = RPC
          .getProtocolVersion(protocol);
    }

    private RequestHeaderProto constructRpcRequestHeader(Method method) {
      RequestHeaderProto.Builder builder = RequestHeaderProto
          .newBuilder();
      builder.setMethodName(method.getName());


      // For protobuf, {@code protocol} used when creating client side proxy is
      // the interface extending BlockingInterface, which has the annotations
      // such as ProtocolName etc.
      //
      // Using Method.getDeclaringClass(), as in WritableEngine to get at
      // the protocol interface will return BlockingInterface, from where
      // the annotation ProtocolName and Version cannot be
      // obtained.
      //
      // Hence we simply use the protocol class used to create the proxy.
      // For PB this may limit the use of mixins on client side.
      builder.setDeclaringClassProtocolName(protocolName);
      builder.setClientProtocolVersion(clientProtocolVersion);
      return builder.build();
    }

    /**
     * This is the client side invoker of RPC method. It only throws
     * ServiceException, since the invocation proxy expects only
     * ServiceException to be thrown by the method in case protobuf service.
     *
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered on the client side in this method are
     * set as cause in ServiceException as is.</li>
     * <li>Exceptions from the server are wrapped in RemoteException and are
     * set as cause in ServiceException</li>
     * </ol>
     *
     * Note that the client calling protobuf RPC methods, must handle
     * ServiceException by getting the cause from the ServiceException. If the
     * cause is RemoteException, then unwrap it to get the exception thrown by
     * the server.
     */
    @Override
    public Message invoke(Object proxy, final Method method, Object[] args)
        throws ServiceException {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.now();
      }

      if (args.length != 2) { // RpcController + Message
        throw new ServiceException(
            "Too many or few parameters for request. Method: ["
            + method.getName() + "]" + ", Expected: 2, Actual: "
            + args.length);
      }
      if (args[1] == null) {
        throw new ServiceException("null param while calling Method: ["
            + method.getName() + "]");
      }

      // if Tracing is on then start a new span for this rpc.
      // guard it in the if statement to make sure there isn't
      // any extra string manipulation.
      Tracer tracer = Tracer.curThreadTracer();
      TraceScope traceScope = null;
      if (tracer != null) {
        traceScope = tracer.newScope(RpcClientUtil.methodToTraceString(method));
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace(Thread.currentThread().getId() + ": Call -> " +
            remoteId + ": " + method.getName() +
            " {" + TextFormat.shortDebugString((Message) args[1]) + "}");
      }


      final Message theRequest = (Message) args[1];
      final RpcWritable.Buffer val;
      try {
        val = (RpcWritable.Buffer) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            constructRpcRequest(method, theRequest), remoteId,
            fallbackToSimpleAuth, alignmentContext);

      } catch (Throwable e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Exception <- " +
              remoteId + ": " + method.getName() +
                " {" + e + "}");
        }
        if (traceScope != null) {
          traceScope.addTimelineAnnotation("Call got exception: " +
              e.toString());
        }
        throw new ServiceException(e);
      } finally {
        if (traceScope != null) {
          traceScope.close();
        }
      }

      if (LOG.isDebugEnabled()) {
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
      }

      if (Client.isAsynchronousMode()) {
        final AsyncGet<RpcWritable.Buffer, IOException> arr
            = Client.getAsyncRpcResponse();
        final AsyncGet<Message, Exception> asyncGet =
            new AsyncGet<Message, Exception>() {
              @Override
              public Message get(long timeout, TimeUnit unit) throws Exception {
                return getReturnMessage(method, arr.get(timeout, unit));
              }

              @Override
              public boolean isDone() {
            return arr.isDone();
          }
        };
        ASYNC_RETURN_MESSAGE.set(asyncGet);
        return null;
      } else {
        return getReturnMessage(method, val);
      }
    }

    protected Writable constructRpcRequest(Method method, Message theRequest) {
      RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
      return new RpcProtobufRequest(rpcRequestHeader, theRequest);
    }

    private Message getReturnMessage(final Method method,
        final RpcWritable.Buffer buf) throws ServiceException {
      Message prototype = null;
      try {
        prototype = getReturnProtoType(method);
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      Message returnMessage;
      try {
        returnMessage = buf.getValue(prototype.getDefaultInstanceForType());

        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Response <- " +
              remoteId + ": " + method.getName() +
                " {" + TextFormat.shortDebugString(returnMessage) + "}");
        }

      } catch (Throwable e) {
        throw new ServiceException(e);
      }
      return returnMessage;
    }

    @Override
    public void close() throws IOException {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    private Message getReturnProtoType(Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      }

      Class<?> returnType = method.getReturnType();
      Method newInstMethod = returnType.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      Message prototype = (Message) newInstMethod.invoke(null, (Object[]) null);
      returnTypes.put(method.getName(), prototype);
      return prototype;
    }

    @Override //RpcInvocationHandler
    public ConnectionId getConnectionId() {
      return remoteId;
    }

    protected long getClientProtocolVersion() {
      return clientProtocolVersion;
    }

    protected String getProtocolName() {
      return protocolName;
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf, SocketFactory.getDefault(),
        RpcWritable.Buffer.class);
  }

  /**
   * todo 在获取到ProtobufRpcEngine2之后，调用其getServer方法，获取Server实例
   *       ProtobufRpcEngine2中的Server二级父类是RPC.Server，一级父类是ipc.Server
   *
   * todo 在整个流程中getServer会调用new Server的构造方法创建Server服务
   * @param protocol protocol协议的类
   * @param protocolImpl protocol实现类
   * @param bindAddress Server绑定的ip地址
   * @param port Server绑定的端口
   * @param numHandlers handler的线程数量，默认值1
   * @param numReaders the number of reader threads to run
   * @param queueSizePerHandler the size of the queue per hander thread
   * @param verbose 是否每一个请求，都需要打印日志
   * @param conf 配置文件
   * @param secretManager The secret manager to use to validate incoming requests.
   * @param portRangeConfig A config parameter that can be used to restrict
   *        the range of ports used when port is 0 (an ephemeral port)
   * @param alignmentContext provides server state info on client responses
   * @return
   * @throws IOException
   */
  @Override
  public RPC.Server getServer(Class<?> protocol, Object protocolImpl,
      String bindAddress, int port, int numHandlers, int numReaders,
      int queueSizePerHandler, boolean verbose, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager,
      String portRangeConfig, AlignmentContext alignmentContext)
      throws IOException {
    return new Server(protocol, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig, alignmentContext);
  }

  @VisibleForTesting
  public static void clearClientCache() {
    CLIENTS.clearCache();
  }

  public static class Server extends RPC.Server {

    static final ThreadLocal<ProtobufRpcEngineCallback2> CURRENT_CALLBACK =
        new ThreadLocal<>();

    static final ThreadLocal<CallInfo> CURRENT_CALL_INFO = new ThreadLocal<>();

    static class CallInfo {
      private final RPC.Server server;
      private final String methodName;

      CallInfo(RPC.Server server, String methodName) {
        this.server = server;
        this.methodName = methodName;
      }

      public RPC.Server getServer() {
        return server;
      }

      public String getMethodName() {
        return methodName;
      }
    }

    static class ProtobufRpcEngineCallbackImpl
        implements ProtobufRpcEngineCallback2 {

      private final RPC.Server server;
      private final Call call;
      private final String methodName;
      private final long setupTime;

      ProtobufRpcEngineCallbackImpl() {
        this.server = CURRENT_CALL_INFO.get().getServer();
        this.call = Server.getCurCall().get();
        this.methodName = CURRENT_CALL_INFO.get().getMethodName();
        this.setupTime = Time.now();
      }

      @Override
      public void setResponse(Message message) {
        long processingTime = Time.now() - setupTime;
        call.setDeferredResponse(RpcWritable.wrap(message));
        server.updateDeferredMetrics(methodName, processingTime);
      }

      @Override
      public void error(Throwable t) {
        long processingTime = Time.now() - setupTime;
        String detailedMetricsName = t.getClass().getSimpleName();
        server.updateDeferredMetrics(detailedMetricsName, processingTime);
        call.setDeferredError(t);
      }
    }

    @InterfaceStability.Unstable
    public static ProtobufRpcEngineCallback2 registerForDeferredResponse2() {
      ProtobufRpcEngineCallback2 callback = new ProtobufRpcEngineCallbackImpl();
      CURRENT_CALLBACK.set(callback);
      return callback;
    }

    /**
     * Construct an RPC server.
     *
     * @param protocolClass the class of protocol
     * @param protocolImpl the protocolImpl whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param portRangeConfig A config parameter that can be used to restrict
     * the range of ports used when port is 0 (an ephemeral port)
     * @param alignmentContext provides server state info on client responses
     *
     * todo 在Server的构建方法中，首先会调用父类RPC.Server的构建方法，然后再调用registerProtocolAndImpl方法注册接口类和接口的实现类
     */
    public Server(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress, int port, int numHandlers,
        int numReaders, int queueSizePerHandler, boolean verbose,
        SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig, AlignmentContext alignmentContext)
        throws IOException {
      super(bindAddress, port, null, numHandlers,
          numReaders, queueSizePerHandler, conf,
          serverNameFromClass(protocolImpl.getClass()), secretManager,
          portRangeConfig);
      setAlignmentContext(alignmentContext);
      this.verbose = verbose;
      // todo 调用registerProtocolAndImpl()方法
      //       注册接口类protocolClass和实现类protocolImpl的映射关系
      registerProtocolAndImpl(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocolClass,
          protocolImpl);
    }

    //Use the latest protobuf rpc invoker itself as that is backward compatible.
    private static final RpcInvoker RPC_INVOKER = new ProtoBufRpcInvoker();

    @Override
    protected RpcInvoker getServerRpcInvoker(RPC.RpcKind rpcKind) {
      if (rpcKind == RPC.RpcKind.RPC_PROTOCOL_BUFFER) {
        return RPC_INVOKER;
      }
      return super.getServerRpcInvoker(rpcKind);
    }

    /**
     * Protobuf invoker for {@link RpcInvoker}.
     */
    static class ProtoBufRpcInvoker implements RpcInvoker {
      private static ProtoClassProtoImpl getProtocolImpl(RPC.Server server,
          String protoName, long clientVersion) throws RpcServerException {
        ProtoNameVer pv = new ProtoNameVer(protoName, clientVersion);
        ProtoClassProtoImpl impl =
            server.getProtocolImplMap(RPC.RpcKind.RPC_PROTOCOL_BUFFER).get(pv);
        if (impl == null) { // no match for Protocol AND Version
          VerProtocolImpl highest = server.getHighestSupportedProtocol(
              RPC.RpcKind.RPC_PROTOCOL_BUFFER, protoName);
          if (highest == null) {
            throw new RpcNoSuchProtocolException(
                "Unknown protocol: " + protoName);
          }
          // protocol supported but not the version that client wants
          throw new RPC.VersionMismatch(protoName, clientVersion,
              highest.version);
        }
        return impl;
      }

      @Override
      /**
       * This is a server side method, which is invoked over RPC. On success
       * the return response has protobuf response payload. On failure, the
       * exception name and the stack trace are returned in the response.
       * See {@link HadoopRpcResponseProto}
       *
       * In this method there three types of exceptions possible and they are
       * returned in response as follows.
       * <ol>
       * <li> Exceptions encountered in this method that are returned
       * as {@link RpcServerException} </li>
       * <li> Exceptions thrown by the service is wrapped in ServiceException.
       * In that this method returns in response the exception thrown by the
       * service.</li>
       * <li> Other exceptions thrown by the service. They are returned as
       * it is.</li>
       * </ol>
       *
       * todo call()方法首先会从请求头中提取出RPC调用的接口名和方法名等信息，
       *      然后根据调用的接口信息获取对应的BlockingService对象，
       *      再根据调用的方法信息在BlockingService对象上调用callBlockingMethod()方法
       *      并将调用前转到ClientNamenodeProtocolServerSideTranslatorPB对象上
       *      最终这个请求会由 NameNodeRpcServer响应。
       */
      public Writable call(RPC.Server server, String connectionProtocolName,
          Writable writableRequest, long receiveTime) throws Exception {
        // todo 获取rpc调用头
        RpcProtobufRequest request = (RpcProtobufRequest) writableRequest;
        RequestHeaderProto rpcRequest = request.getRequestHeader();
        // todo 获得调用的接口名、方法名、版本号
        String methodName = rpcRequest.getMethodName();

        /**
         * RPCs for a particular interface (ie protocol) are done using a
         * IPC connection that is setup using rpcProxy.
         * The rpcProxy's has a declared protocol name that is
         * sent form client to server at connection time.
         *
         * Each Rpc call also sends a protocol name
         * (called declaringClassprotocolName). This name is usually the same
         * as the connection protocol name except in some cases.
         * For example metaProtocols such ProtocolInfoProto which get info
         * about the protocol reuse the connection but need to indicate that
         * the actual protocol is different (i.e. the protocol is
         * ProtocolInfoProto) since they reuse the connection; in this case
         * the declaringClassProtocolName field is set to the ProtocolInfoProto.
         */

        String declaringClassProtoName =
            rpcRequest.getDeclaringClassProtocolName();
        long clientVersion = rpcRequest.getClientProtocolVersion();
        return call(server, connectionProtocolName, request, receiveTime,
            methodName, declaringClassProtoName, clientVersion);
      }

      @SuppressWarnings("deprecation")
      protected Writable call(RPC.Server server, String connectionProtocolName,
          RpcWritable.Buffer request, long receiveTime, String methodName,
          String declaringClassProtoName, long clientVersion) throws Exception {
        if (server.verbose) {
          LOG.info("Call: connectionProtocolName=" + connectionProtocolName +
              ", method=" + methodName);
        }

        // todo 获得该接口在Server侧对应的实现类
        ProtoClassProtoImpl protocolImpl = getProtocolImpl(server,
                              declaringClassProtoName, clientVersion);
        if (protocolImpl.isShadedPBImpl()) {
          return call(server, connectionProtocolName, request, methodName,
              protocolImpl);
        }
        //Legacy protobuf implementation. Handle using legacy (Non-shaded)
        // protobuf classes.
        return ProtobufRpcEngine.Server
            .processCall(server, connectionProtocolName, request, methodName,
                protocolImpl);
      }

      private RpcWritable call(RPC.Server server,
          String connectionProtocolName, RpcWritable.Buffer request,
          String methodName, ProtoClassProtoImpl protocolImpl)
          throws Exception {
        BlockingService service = (BlockingService) protocolImpl.protocolImpl;
        //todo 获取要调用的方法的描述信息
        MethodDescriptor methodDescriptor = service.getDescriptorForType()
            .findMethodByName(methodName);
        if (methodDescriptor == null) {
          String msg = "Unknown method " + methodName + " called on "
                                + connectionProtocolName + " protocol.";
          LOG.warn(msg);
          throw new RpcNoSuchMethodException(msg);
        }
        // todo 获取调用的方法描述符以及调用参数
        Message prototype = service.getRequestPrototype(methodDescriptor);
        Message param = request.getValue(prototype);

        Message result;
        Call currentCall = Server.getCurCall().get();
        try {
          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
          CURRENT_CALL_INFO.set(new CallInfo(server, methodName));
          currentCall.setDetailedMetricsName(methodName);
          // todo 在实现类上调用callBlockingMethod方法，级联适配调用到NameNodeRpcServer
          result = service.callBlockingMethod(methodDescriptor, null, param);
          // Check if this needs to be a deferred response,
          // by checking the ThreadLocal callback being set
          if (CURRENT_CALLBACK.get() != null) {
            currentCall.deferResponse();
            CURRENT_CALLBACK.set(null);
            return null;
          }
        } catch (ServiceException e) {
          Exception exception = (Exception) e.getCause();
          currentCall.setDetailedMetricsName(
              exception.getClass().getSimpleName());
          throw (Exception) e.getCause();
        } catch (Exception e) {
          currentCall.setDetailedMetricsName(e.getClass().getSimpleName());
          throw e;
        } finally {
          CURRENT_CALL_INFO.set(null);
        }
        return RpcWritable.wrap(result);
      }
    }
  }

  // htrace in the ipc layer creates the span name based on toString()
  // which uses the rpc header.  in the normal case we want to defer decoding
  // the rpc header until needed by the rpc engine.
  static class RpcProtobufRequest extends RpcWritable.Buffer {
    private volatile RequestHeaderProto requestHeader;
    private Message payload;

    RpcProtobufRequest() {
    }

    RpcProtobufRequest(RequestHeaderProto header, Message payload) {
      this.requestHeader = header;
      this.payload = payload;
    }

    RequestHeaderProto getRequestHeader() throws IOException {
      if (getByteBuffer() != null && requestHeader == null) {
        requestHeader = getValue(RequestHeaderProto.getDefaultInstance());
      }
      return requestHeader;
    }

    @Override
    public void writeTo(ResponseBuffer out) throws IOException {
      requestHeader.writeDelimitedTo(out);
      if (payload != null) {
        payload.writeDelimitedTo(out);
      }
    }

    // this is used by htrace to name the span.
    @Override
    public String toString() {
      try {
        RequestHeaderProto header = getRequestHeader();
        return header.getDeclaringClassProtocolName() + "." +
               header.getMethodName();
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
