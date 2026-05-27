/*
 * Copyright 2026 The Netty VirtualThread Scheduler Project
 *
 * The Netty VirtualThread Scheduler Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package io.netty.loom.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.web.server.WebServerException;
import org.springframework.boot.web.servlet.ServletContextInitializer;

import com.example.dispatcher.DispatcherHttpServletRequest;
import com.example.dispatcher.DispatcherHttpServletResponse;
import com.example.dispatcher.DispatcherServletContext;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.loom.VirtualIoNativePollerEventLoopGroup;
import jakarta.servlet.ServletException;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * Netty-based web server that uses our
 * {@link VirtualIoNativePollerEventLoopGroup} with epoll pinned pollers.
 * Servlet requests are dispatched to virtual threads with carrier affinity —
 * the blocking {@code @RestController} handler runs on a VT created by the
 * scheduler's factory.
 */
public class LoomNettyWebServer implements WebServer {

	private static final Log logger = LogFactory.getLog(LoomNettyWebServer.class);

	private final DispatcherServletContext servletContext = new DispatcherServletContext();
	private final int port;
	private ChannelFuture server;
	private VirtualIoNativePollerEventLoopGroup group;
	private Thread serverThread;

	public LoomNettyWebServer(int port, ServletContextInitializer[] initializers) {
		this.port = port;
		try {
			for (var initializer : initializers) {
				initializer.onStartup(servletContext);
			}
		} catch (ServletException e) {
			throw new IllegalStateException("Cannot initialize", e);
		}
	}

	@Override
	public void start() throws WebServerException {
		this.group = new VirtualIoNativePollerEventLoopGroup(EpollIoHandler.newFactory());
		try {
			var bootstrap = new ServerBootstrap();
			bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
			bootstrap.group(this.group).channel(EpollServerSocketChannel.class)
					.childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
						public void initChannel(SocketChannel ch) {
							ch.pipeline().addLast(new HttpServerCodec());
							ch.pipeline().addLast(new ServletHandler(servletContext, group));
						}
					});
			this.server = bootstrap.bind(this.port).sync();
			logger.info("Netty (epoll, loom scheduler) started on port " + getPort());
			this.serverThread = Thread.ofPlatform().daemon(false).name("server").start(() -> {
				try {
					server.channel().closeFuture().sync();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			cleanup();
			throw new WebServerException("Cannot start server", e);
		} catch (RuntimeException e) {
			cleanup();
			throw e;
		}
	}

	@Override
	public void stop() throws WebServerException {
		if (this.server != null) {
			this.server.cancel(true);
			this.server = null;
		}
		cleanup();
	}

	private void cleanup() {
		if (this.group != null) {
			this.group.close();
			this.group = null;
		}
	}

	@Override
	public int getPort() {
		return server == null ? this.port : ((InetSocketAddress) server.channel().localAddress()).getPort();
	}

	/**
	 * Netty channel handler that dispatches HTTP requests to the servlet filter
	 * chain on a virtual thread from our scheduler.
	 */
	static class ServletHandler extends ChannelInboundHandlerAdapter {

		private final DispatcherServletContext servletContext;
		private final VirtualIoNativePollerEventLoopGroup group;
		private HttpRequest request;
		private byte[] body;

		ServletHandler(DispatcherServletContext servletContext, VirtualIoNativePollerEventLoopGroup group) {
			this.servletContext = servletContext;
			this.group = group;
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			if (msg instanceof HttpRequest httpRequest) {
				this.request = httpRequest;
			}
			if (msg instanceof HttpContent httpContent) {
				this.body = new byte[httpContent.content().readableBytes()];
				httpContent.content().readBytes(this.body);
				httpContent.content().release();

				if (msg instanceof LastHttpContent) {
					final var capturedRequest = this.request;
					final var capturedBody = this.body;
					group.vThreadFactory().newThread(() -> {
						dispatchServlet(ctx, capturedRequest, capturedBody);
					}).start();
					this.request = null;
					this.body = null;
				}
			}
		}

		private void dispatchServlet(ChannelHandlerContext ctx, HttpRequest request, byte[] body) {
			var servletRequest = new DispatcherHttpServletRequest(servletContext);
			var servletResponse = new DispatcherHttpServletResponse();

			servletRequest.setHeaders(toMultiValueMap(request.headers()));
			servletRequest.setMethod(request.method().name());
			servletRequest.setRequestURI(request.uri());
			servletRequest.setParameters(RequestUtils.formatParams(request));
			servletRequest.setContent(body);
			var responseHeaderMap = new LinkedMultiValueMap<String, String>();
			servletResponse.setHeaders(responseHeaderMap);

			try {
				servletContext.filterChain().doFilter(servletRequest, servletResponse);
			} catch (IOException | ServletException e) {
				throw new IllegalStateException("Servlet dispatch failed", e);
			}

			boolean keepAlive = HttpUtil.isKeepAlive(request);
			byte[] responseBody = servletResponse.getContentAsByteArray();
			var status = HttpResponseStatus.valueOf(servletResponse.getStatus());

			var httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
					Unpooled.copiedBuffer(responseBody));
			responseHeaderMap.forEach((name, values) -> httpResponse.headers().add(name, values));
			if (!httpResponse.headers().contains(HttpHeaderNames.CONTENT_LENGTH)) {
				httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, responseBody.length);
			}
			if (keepAlive && !httpResponse.headers().contains(HttpHeaderNames.CONNECTION)) {
				httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
			}

			ctx.writeAndFlush(httpResponse);
			if (!keepAlive) {
				ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			cause.printStackTrace();
			ctx.close();
		}
	}

	private static MultiValueMap<String, String> toMultiValueMap(io.netty.handler.codec.http.HttpHeaders headers) {
		var map = new LinkedMultiValueMap<String, String>();
		for (var entry : headers) {
			map.add(entry.getKey(), entry.getValue());
		}
		return map;
	}

	static class RequestUtils {

		static java.util.Map<String, String[]> formatParams(HttpRequest request) {
			var decoder = new QueryStringDecoder(request.uri());
			var params = decoder.parameters();
			var result = new java.util.HashMap<String, String[]>();
			for (var entry : params.entrySet()) {
				result.put(entry.getKey(), entry.getValue().toArray(new String[0]));
			}
			return result;
		}
	}
}
