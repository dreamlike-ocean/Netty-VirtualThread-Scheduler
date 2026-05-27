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

import java.io.File;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.web.error.ErrorPage;
import org.springframework.boot.web.server.Compression;
import org.springframework.boot.web.server.Http2;
import org.springframework.boot.web.server.MimeMappings;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.web.server.autoconfigure.ServerProperties;
import org.springframework.boot.web.server.servlet.ConfigurableServletWebServerFactory;
import org.springframework.boot.web.server.servlet.Jsp;
import org.springframework.boot.web.server.servlet.ServletWebServerSettings;
import org.springframework.boot.web.server.servlet.Session;
import org.springframework.boot.web.servlet.ServletContextInitializer;

/**
 * Servlet web server factory that creates a {@link LoomNettyWebServer} backed
 * by our {@link io.netty.loom.VirtualIoNativePollerEventLoopGroup}.
 */
public class LoomNettyWebServerFactory implements ConfigurableServletWebServerFactory {

	private final ServerProperties server;
	private final ServletWebServerSettings settings = new ServletWebServerSettings();

	public LoomNettyWebServerFactory(ServerProperties server) {
		this.server = server;
	}

	@Override
	public ServletWebServerSettings getSettings() {
		return this.settings;
	}

	@Override
	public WebServer getWebServer(ServletContextInitializer... initializers) {
		return new LoomNettyWebServer(server.getPort() != null ? server.getPort() : 8080, initializers);
	}

	@Override
	public void setPort(int port) {
	}

	@Override
	public void setAddress(InetAddress address) {
	}

	@Override
	public void setErrorPages(Set<? extends ErrorPage> errorPages) {
	}

	@Override
	public void setSsl(Ssl ssl) {
	}

	@Override
	public void setSslBundles(SslBundles sslStoreProvider) {
	}

	@Override
	public void setHttp2(Http2 http2) {
	}

	@Override
	public void setCompression(Compression compression) {
	}

	@Override
	public void setServerHeader(String serverHeader) {
	}

	@Override
	public void addErrorPages(ErrorPage... errorPages) {
	}

	@Override
	public void setContextPath(String contextPath) {
	}

	@Override
	public void setDisplayName(String displayName) {
	}

	@Override
	public void setSession(Session session) {
	}

	@Override
	public void setRegisterDefaultServlet(boolean registerDefaultServlet) {
	}

	@Override
	public void setMimeMappings(MimeMappings mimeMappings) {
	}

	@Override
	public void setDocumentRoot(File documentRoot) {
	}

	@Override
	public void setInitializers(List<? extends ServletContextInitializer> initializers) {
	}

	@Override
	public void addInitializers(ServletContextInitializer... initializers) {
	}

	@Override
	public void setJsp(Jsp jsp) {
	}

	@Override
	public void setLocaleCharsetMappings(Map<Locale, Charset> localeCharsetMappings) {
	}

	@Override
	public void setInitParameters(Map<String, String> initParameters) {
	}
}
