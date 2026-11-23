//
//  ========================================================================
//  Copyright (c) 1995-2018 Mort Bay Consulting Pty. Ltd.
//  ------------------------------------------------------------------------
//  All rights reserved. This program and the accompanying materials
//  are made available under the terms of the Eclipse Public License v1.0
//  and Apache License v2.0 which accompanies this distribution.
//
//      The Eclipse Public License is available at
//      http://www.eclipse.org/legal/epl-v10.html
//
//      The Apache License v2.0 is available at
//      http://www.opensource.org/licenses/apache2.0.php
//
//  You may elect to redistribute this code under either of these licenses.
//  ========================================================================
//

package org.eclipse.jetty.proxy;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpHeaderValue;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.ManagedSelector;
import org.eclipse.jetty.io.MappedByteBufferPool;
import org.eclipse.jetty.io.SelectChannelEndPoint;
import org.eclipse.jetty.io.SelectorManager;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.HttpTransport;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.HostPort;
import org.eclipse.jetty.util.Promise;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.TypeUtil;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.util.thread.Scheduler;

/**
 * <p>Implementation of a {@link Handler} that supports HTTP CONNECT.</p>
 */
public class ConnectHandler extends HandlerWrapper
{
    protected static final Logger LOG = Log.getLogger(ConnectHandler.class);

    private final Set<String> whiteList = new HashSet<>();
    private final Set<String> blackList = new HashSet<>();
    private Executor executor;
    private Scheduler scheduler;
    private ByteBufferPool bufferPool;
    private SelectorManager selector;
    private long connectTimeout = 15000;
    private long idleTimeout = 30000;
    private int bufferSize = 4096;

    public ConnectHandler()
    {
        this(null);
    }

    public ConnectHandler(Handler handler)
    {
        setHandler(handler);
    }

    public Executor getExecutor()
    {
        return executor;
    }

    public void setExecutor(Executor executor)
    {
        this.executor = executor;
    }

    public Scheduler getScheduler()
    {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler)
    {
        this.scheduler = scheduler;
    }

    public ByteBufferPool getByteBufferPool()
    {
        return bufferPool;
    }

    public void setByteBufferPool(ByteBufferPool bufferPool)
    {
        this.bufferPool = bufferPool;
    }

    /**
     * @return the timeout, in milliseconds, to connect to the remote server
     */
    public long getConnectTimeout()
    {
        return connectTimeout;
    }

    /**
     * @param connectTimeout the timeout, in milliseconds, to connect to the remote server
     */
    public void setConnectTimeout(long connectTimeout)
    {
        this.connectTimeout = connectTimeout;
    }

    /**
     * @return the idle timeout, in milliseconds
     */
    public long getIdleTimeout()
    {
        return idleTimeout;
    }

    /**
     * @param idleTimeout the idle timeout, in milliseconds
     */
    public void setIdleTimeout(long idleTimeout)
    {
        this.idleTimeout = idleTimeout;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    @Override
    protected void doStart() throws Exception
    {
        if (executor == null)
            executor = getServer().getThreadPool();

        if (scheduler == null)
            addBean(scheduler = new ScheduledExecutorScheduler());

        if (bufferPool == null)
            addBean(bufferPool = new MappedByteBufferPool());

        addBean(selector = newSelectorManager());
        selector.setConnectTimeout(getConnectTimeout());

        super.doStart();
    }

    protected SelectorManager newSelectorManager()
    {
        return new ConnectManager(getExecutor(), getScheduler(), 1);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        if (HttpMethod.CONNECT.is(request.getMethod()))
        {
            HttpURI uri = baseRequest.getHttpURI();
            String serverAddress = getServerAddress(uri);
            if (LOG.isDebugEnabled())
                LOG.debug("CONNECT request for {}", serverAddress);

            if (serverAddress == null)
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("CONNECT request-target not in authority-form: {}", uri);
                baseRequest.setHandled(true);
                sendConnectResponse(request, response, HttpServletResponse.SC_BAD_REQUEST);
                return;
            }

            handleConnect(baseRequest, request, response, serverAddress);
        }
        else
        {
            super.handle(target, baseRequest, request, response);
        }
    }

    /**
     * <p>Returns the address of the remote server of a CONNECT request,
     * taken from the request-target, that must be in authority-form ({@code host:port}).</p>
     * <p>When the request-target has been parsed as an authority, the URI authority is used;
     * when it has been retained verbatim as the URI path, the path is used, so that
     * an authority derived from the {@code Host} header is never used instead.</p>
     *
     * @param uri the CONNECT request URI
     * @return the remote server address, or null if the request-target is not in authority-form
     */
    private static String getServerAddress(HttpURI uri)
    {
        if (uri == null)
            return null;
        String path = uri.getPath();
        String serverAddress;
        if (path == null || path.isEmpty())
            serverAddress = uri.getHost() == null ? null : uri.getAuthority();
        else
            serverAddress = path;
        return isAuthorityForm(serverAddress) ? serverAddress : null;
    }

    /**
     * @param authority the CONNECT request-target
     * @return whether the given string is a {@code host[:port]} authority with
     * a non empty host, no user info, no path, no query and no fragment
     */
    private static boolean isAuthorityForm(String authority)
    {
        if (authority == null || authority.isEmpty())
            return false;

        int length = authority.length();
        int hostEnd;
        if (authority.charAt(0) == '[')
        {
            // IP-literal.
            hostEnd = authority.indexOf(']') + 1;
            if (hostEnd < 4)
                return false;
            boolean colon = false;
            for (int i = 1; i < hostEnd - 1; ++i)
            {
                char c = authority.charAt(i);
                if (c == ':')
                    colon = true;
                else if (!isHostChar(c) && c != '%')
                    return false;
            }
            if (!colon)
                return false;
        }
        else
        {
            // IPv4 address or host name.
            hostEnd = authority.indexOf(':');
            if (hostEnd < 0)
                hostEnd = length;
            if (hostEnd == 0)
                return false;
            for (int i = 0; i < hostEnd; ++i)
            {
                if (!isHostChar(authority.charAt(i)))
                    return false;
            }
        }

        if (hostEnd == length)
            return true;
        if (authority.charAt(hostEnd) != ':')
            return false;

        // Port.
        int portStart = hostEnd + 1;
        if (portStart == length || length - portStart > 5)
            return false;
        int port = 0;
        for (int i = portStart; i < length; ++i)
        {
            char c = authority.charAt(i);
            if (c < '0' || c > '9')
                return false;
            port = port * 10 + (c - '0');
        }
        return port > 0 && port <= 65535;
    }

    private static boolean isHostChar(char c)
    {
        return (c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') ||
                c == '-' || c == '.' || c == '_';
    }

    /**
     * <p>Handles a CONNECT request.</p>
     * <p>CONNECT requests may have authentication headers such as {@code Proxy-Authorization}
     * that authenticate the client with the proxy.</p>
     *
     * @param baseRequest  Jetty-specific http request
     * @param request       the http request
     * @param response      the http response
     * @param serverAddress the remote server address in the form {@code host:port}
     */
    protected void handleConnect(Request baseRequest, HttpServletRequest request, HttpServletResponse response, String serverAddress)
    {
        baseRequest.setHandled(true);
        try
        {
            boolean proceed = handleAuthentication(request, response, serverAddress);
            if (!proceed)
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("Missing proxy authentication");
                sendConnectResponse(request, response, HttpServletResponse.SC_PROXY_AUTHENTICATION_REQUIRED);
                return;
            }

            HostPort hostPort = new HostPort(serverAddress);
            String host = hostPort.getHost();
            int port = hostPort.getPort(80);

            if (!validateDestination(host, port))
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("Destination {}:{} forbidden", host, port);
                sendConnectResponse(request, response, HttpServletResponse.SC_FORBIDDEN);
                return;
            }

            HttpTransport transport = baseRequest.getHttpChannel().getHttpTransport();
            // TODO Handle CONNECT over HTTP2!
            if (!(transport instanceof HttpConnection))
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("CONNECT not supported for {}", transport);
                sendConnectResponse(request, response, HttpServletResponse.SC_FORBIDDEN);
                return;
            }

            AsyncContext asyncContext = request.startAsync();
            asyncContext.setTimeout(0);

            if (LOG.isDebugEnabled())
                LOG.debug("Connecting to {}:{}", host, port);

            connectToServer(request, host, port, new Promise<SocketChannel>()
            {
                @Override
                public void succeeded(SocketChannel channel)
                {
                    ConnectContext connectContext = new ConnectContext(request, response, asyncContext, (HttpConnection)transport);
                    if (channel.isConnected())
                        selector.accept(channel, connectContext);
                    else
                        selector.connect(channel, connectContext);
                }

                @Override
                public void failed(Throwable x)
                {
                    onConnectFailure(request, response, asyncContext, x);
                }
            });
        }
        catch (Exception x)
        {
            onConnectFailure(request, response, null, x);
        }
    }

    protected void connectToServer(HttpServletRequest request, String host, int port, Promise<SocketChannel> promise)
    {
        SocketChannel channel = null;
        try
        {
            channel = SocketChannel.open();
            channel.socket().setTcpNoDelay(true);
            channel.configureBlocking(false);
            InetSocketAddress address = newConnectAddress(host, port);
            channel.connect(address);
            promise.succeeded(channel);
        }
        catch (Throwable x)
        {
            close(channel);
            promise.failed(x);
        }
    }

    private void close(Closeable closeable)
    {
        try
        {
            if (closeable != null)
                closeable.close();
        }
        catch (Throwable x)
        {
            LOG.ignore(x);
        }
    }

    /**
     * Creates the server address to connect to.
     *
     * @param host The host from the CONNECT request
     * @param port The port from the CONNECT request
     * @return The InetSocketAddress to connect to.
     */
    protected InetSocketAddress newConnectAddress(String host, int port)
    {
        return new InetSocketAddress(host, port);
    }

    protected void onConnectSuccess(ConnectContext connectContext, UpstreamConnection upstreamConnection)
    {
        ConcurrentMap<String, Object> context = connectContext.getContext();
        HttpServletRequest request = connectContext.getRequest();
        prepareContext(request, context);

        HttpConnection httpConnection = connectContext.getHttpConnection();
        EndPoint downstreamEndPoint = httpConnection.getEndPoint();
        DownstreamConnection downstreamConnection = newDownstreamConnection(downstreamEndPoint, context, BufferUtil.EMPTY_BUFFER);
        downstreamConnection.setInputBufferSize(getBufferSize());

        upstreamConnection.setConnection(downstreamConnection);
        downstreamConnection.setConnection(upstreamConnection);
        if (LOG.isDebugEnabled())
            LOG.debug("Connection setup completed: {}<->{}", downstreamConnection, upstreamConnection);

        HttpServletResponse response = connectContext.getResponse();
        sendConnectResponse(request, response, HttpServletResponse.SC_OK);

        upgradeConnection(request, response, downstreamConnection);

        connectContext.getAsyncContext().complete();
    }

    protected void onConnectFailure(HttpServletRequest request, HttpServletResponse response, AsyncContext asyncContext, Throwable failure)
    {
        if (LOG.isDebugEnabled())
            LOG.debug("CONNECT failed", failure);
        sendConnectResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        if (asyncContext != null)
            asyncContext.complete();
    }

    private void sendConnectResponse(HttpServletRequest request, HttpServletResponse response, int statusCode)
    {
        try
        {
            response.setStatus(statusCode);
            response.setContentLength(0);
            if (statusCode != HttpServletResponse.SC_OK)
                response.setHeader(HttpHeader.CONNECTION.asString(), HttpHeaderValue.CLOSE.asString());
            response.getOutputStream().close();
            if (LOG.isDebugEnabled())
                LOG.debug("CONNECT response sent {} {}", request.getProtocol(), response.getStatus());
        }
        catch (IOException x)
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Could not send CONNECT response", x);
        }
    }

    /**
     * <p>Handles the authentication before setting up the tunnel to the remote server.</p>
     * <p>The default implementation returns true.</p>
     *
     * @param request  the HTTP request
     * @param response the HTTP response
     * @param address  the address of the remote server in the form {@code host:port}.
     * @return true to allow to connect to the remote host, false otherwise
     */
    protected boolean handleAuthentication(HttpServletRequest request, HttpServletResponse response, String address)
    {
        return true;
    }

    /**
     * @deprecated use {@link #newDownstreamConnection(EndPoint, ConcurrentMap)} instead
     */
    @Deprecated
    protected DownstreamConnection newDownstreamConnection(EndPoint endPoint, ConcurrentMap<String, Object> context, ByteBuffer buffer)
    {
        return newDownstreamConnection(endPoint, context);
    }

    protected DownstreamConnection newDownstreamConnection(EndPoint endPoint, ConcurrentMap<String, Object> context)
    {
        return new DownstreamConnection(endPoint, getExecutor(), getByteBufferPool(), context);
    }

    protected UpstreamConnection newUpstreamConnection(EndPoint endPoint, ConnectContext connectContext)
    {
        return new UpstreamConnection(endPoint, getExecutor(), getByteBufferPool(), connectContext);
    }

    protected void prepareContext(HttpServletRequest request, ConcurrentMap<String, Object> context)
    {
    }

    private void upgradeConnection(HttpServletRequest request, HttpServletResponse response, Connection connection)
    {
        // Set the new connection as request attribute and change the status to 101
        // so that Jetty understands that it has to upgrade the connection
        request.setAttribute(HttpConnection.UPGRADE_CONNECTION_ATTRIBUTE, connection);
        response.setStatus(HttpServletResponse.SC_SWITCHING_PROTOCOLS);
        if (LOG.isDebugEnabled())
            LOG.debug("Upgraded connection to {}", connection);
    }

    /**
     * <p>Reads (with non-blocking semantic) into the given {@code buffer} from the given {@code endPoint}.</p>
     *
     * @param endPoint the endPoint to read from
     * @param buffer   the buffer to read data into
     * @param context  the context information related to the connection
     * @return the number of bytes read (possibly 0 since the read is non-blocking)
     *         or -1 if the channel has been closed remotely
     * @throws IOException if the endPoint cannot be read
     */
    protected int read(EndPoint endPoint, ByteBuffer buffer, ConcurrentMap<String, Object> context) throws IOException
    {
        int read = read(endPoint, buffer);
        if (LOG.isDebugEnabled())
            LOG.debug("{} read {} bytes", this, read);
        return read;
    }

    /**
     * @deprecated override {@link #read(EndPoint, ByteBuffer, ConcurrentMap)} instead
     */
    @Deprecated
    protected int read(EndPoint endPoint, ByteBuffer buffer) throws IOException
    {
        return endPoint.fill(buffer);
    }

    /**
     * <p>Writes (with non-blocking semantic) the given buffer of data onto the given endPoint.</p>
     *
     * @param endPoint the endPoint to write to
     * @param buffer   the buffer to write
     * @param callback the completion callback to invoke
     * @param context  the context information related to the connection
     */
    protected void write(EndPoint endPoint, ByteBuffer buffer, Callback callback, ConcurrentMap<String, Object> context)
    {
        if (LOG.isDebugEnabled())
            LOG.debug("{} writing {} bytes", this, buffer.remaining());
        write(endPoint, buffer, callback);
    }

    /**
     * @deprecated override {@link #write(EndPoint, ByteBuffer, Callback, ConcurrentMap)} instead
     */
    @Deprecated
    protected void write(EndPoint endPoint, ByteBuffer buffer, Callback callback)
    {
        endPoint.write(callback, buffer);
    }

    public Set<String> getWhiteListHosts()
    {
        return whiteList;
    }

    public Set<String> getBlackListHosts()
    {
        return blackList;
    }

    /**
     * Checks the given {@code host} and {@code port} against whitelist and blacklist.
     * <p>Host names are case insensitive, so the comparison is done ignoring the ASCII case.</p>
     *
     * @param host the host to check
     * @param port the port to check
     * @return true if it is allowed to connect to the given host and port
     */
    public boolean validateDestination(String host, int port)
    {
        String hostPort = StringUtil.asciiToLowerCase(host + ":" + port);
        if (!whiteList.isEmpty())
        {
            if (!containsHostPort(whiteList, hostPort))
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("Host {}:{} not whitelisted", host, port);
                return false;
            }
        }
        if (!blackList.isEmpty())
        {
            if (containsHostPort(blackList, hostPort))
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("Host {}:{} blacklisted", host, port);
                return false;
            }
        }
        return true;
    }

    /**
     * @param hosts the set of {@code host:port} entries
     * @param hostPort the ASCII lower case {@code host:port} to look for
     * @return whether the given set contains an entry equal to {@code hostPort}, ignoring the ASCII case
     */
    private static boolean containsHostPort(Set<String> hosts, String hostPort)
    {
        for (String entry : hosts)
        {
            if (hostPort.equals(StringUtil.asciiToLowerCase(entry)))
                return true;
        }
        return false;
    }

    @Override
    public void dump(Appendable out, String indent) throws IOException
    {
        dumpThis(out);
        dump(out, indent, getBeans(), TypeUtil.asList(getHandlers()));
    }

    protected class ConnectManager extends SelectorManager
    {
        protected ConnectManager(Executor executor, Scheduler scheduler, int selectors)
        {
            super(executor, scheduler, selectors);
        }

        @Override
        protected EndPoint newEndPoint(SocketChannel channel, ManagedSelector selector, SelectionKey selectionKey) throws IOException
        {
            return new SelectChannelEndPoint(channel, selector, selectionKey, getScheduler(), getIdleTimeout());
        }

        @Override
        public Connection newConnection(SocketChannel channel, EndPoint endpoint, Object attachment) throws IOException
        {
            if (ConnectHandler.LOG.isDebugEnabled())
                ConnectHandler.LOG.debug("Connected to {}", channel.getRemoteAddress());
            ConnectContext connectContext = (ConnectContext)attachment;
            UpstreamConnection connection = newUpstreamConnection(endpoint, connectContext);
            connection.setInputBufferSize(getBufferSize());
            return connection;
        }

        @Override
        protected void connectionFailed(SocketChannel channel, final Throwable ex, final Object attachment)
        {
            close(channel);
            ConnectContext connectContext = (ConnectContext)attachment;
            onConnectFailure(connectContext.request, connectContext.response, connectContext.asyncContext, ex);
        }
    }

    protected static class ConnectContext
    {
        private final ConcurrentMap<String, Object> context = new ConcurrentHashMap<>();
        private final HttpServletRequest request;
        private final HttpServletResponse response;
        private final AsyncContext asyncContext;
        private final HttpConnection httpConnection;

        public ConnectContext(HttpServletRequest request, HttpServletResponse response, AsyncContext asyncContext, HttpConnection httpConnection)
        {
            this.request = request;
            this.response = response;
            this.asyncContext = asyncContext;
            this.httpConnection = httpConnection;
        }

        public ConcurrentMap<String, Object> getContext()
        {
            return context;
        }

        public HttpServletRequest getRequest()
        {
            return request;
        }

        public HttpServletResponse getResponse()
        {
            return response;
        }

        public AsyncContext getAsyncContext()
        {
            return asyncContext;
        }

        public HttpConnection getHttpConnection()
        {
            return httpConnection;
        }
    }

    public class UpstreamConnection extends ProxyConnection
    {
        private ConnectContext connectContext;

        public UpstreamConnection(EndPoint endPoint, Executor executor, ByteBufferPool bufferPool, ConnectContext connectContext)
        {
            super(endPoint, executor, bufferPool, connectContext.getContext());
            this.connectContext = connectContext;
        }

        @Override
        public void onOpen()
        {
            super.onOpen();
            onConnectSuccess(connectContext, UpstreamConnection.this);
            fillInterested();
        }

        @Override
        protected int read(EndPoint endPoint, ByteBuffer buffer) throws IOException
        {
            return ConnectHandler.this.read(endPoint, buffer, getContext());
        }

        @Override
        protected void write(EndPoint endPoint, ByteBuffer buffer,Callback callback)
        {
            ConnectHandler.this.write(endPoint, buffer, callback, getContext());
        }
    }

    public class DownstreamConnection extends ProxyConnection implements Connection.UpgradeTo
    {
        private ByteBuffer buffer;

        public DownstreamConnection(EndPoint endPoint, Executor executor, ByteBufferPool bufferPool, ConcurrentMap<String, Object> context)
        {
            super(endPoint, executor, bufferPool, context);
        }

        /**
         * @deprecated use {@link #DownstreamConnection(EndPoint, Executor, ByteBufferPool, ConcurrentMap)} instead
         */
        @Deprecated
        public DownstreamConnection(EndPoint endPoint, Executor executor, ByteBufferPool bufferPool, ConcurrentMap<String, Object> context, ByteBuffer buffer)
        {
            this(endPoint, executor, bufferPool, context);
        }

        @Override
        public void onUpgradeTo(ByteBuffer buffer)
        {
            this.buffer = buffer == null ? BufferUtil.EMPTY_BUFFER : buffer;
        }

        @Override
        public void onOpen()
        {
            super.onOpen();
            final int remaining = buffer.remaining();
            write(getConnection().getEndPoint(), buffer, new Callback()
            {
                @Override
                public void succeeded()
                {
                    if (LOG.isDebugEnabled())
                        LOG.debug("{} wrote initial {} bytes to server", DownstreamConnection.this, remaining);
                    fillInterested();
                }

                @Override
                public void failed(Throwable x)
                {
                    if (LOG.isDebugEnabled())
                        LOG.debug(this + " failed to write initial " + remaining + " bytes to server", x);
                    close();
                    getConnection().close();
                }
            });
        }

        @Override
        protected int read(EndPoint endPoint, ByteBuffer buffer) throws IOException
        {
            return ConnectHandler.this.read(endPoint, buffer, getContext());
        }

        @Override
        protected void write(EndPoint endPoint, ByteBuffer buffer, Callback callback)
        {
            ConnectHandler.this.write(endPoint, buffer, callback, getContext());
        }
    }
}
