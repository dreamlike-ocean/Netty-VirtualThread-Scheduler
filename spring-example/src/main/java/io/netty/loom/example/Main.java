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

import static java.util.concurrent.StructuredTaskScope.Joiner.allSuccessfulOrThrow;

import java.util.concurrent.StructuredTaskScope;

import io.netty.loom.scheduler.EventLoopScheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Spring Boot MVC application running on Netty with the Netty VirtualThread
 * Scheduler.
 *
 * <p>
 * Uses {@code dsyer/dispatcher-servlet-container} to bridge Spring MVC's
 * {@code DispatcherServlet} onto Netty, replacing the default event loop group
 * with our {@link io.netty.loom.VirtualIoNativePollerEventLoopGroup} backed by
 * epoll pinned pollers. Blocking {@code @RestController} handlers run on
 * virtual threads with carrier affinity.
 */
@SpringBootApplication(proxyBeanMethods = false)
@RestController
public class Main {

	public static void main(String[] args) {
		System.setProperty("jdk.virtualThreadScheduler.implClass", "io.netty.loom.scheduler.NettyScheduler");
		SpringApplication.run(Main.class, args);
	}

	@GetMapping("/")
	public String hello() throws InterruptedException {
		var scheduler = EventLoopScheduler.currentScheduler();
		Thread.sleep(50);
		return "HELLO from " + Thread.currentThread() + " on carrier " + scheduler.carrierThread().getName()
				+ " (scheduler " + scheduler.id() + ")";
	}

	@GetMapping("/parallel")
	public String parallel() throws Exception {
		var scheduler = EventLoopScheduler.currentScheduler();
		var factory = scheduler.virtualThreadFactory();
		try (var scope = StructuredTaskScope.open(allSuccessfulOrThrow(), cfg -> cfg.withThreadFactory(factory))) {
			var taskA = scope.fork(() -> {
				Thread.sleep(50);
				return "A from " + Thread.currentThread();
			});
			var taskB = scope.fork(() -> {
				Thread.sleep(50);
				return "B from " + Thread.currentThread();
			});
			scope.join();
			return taskA.get() + " | " + taskB.get();
		}
	}

	@GetMapping("/health")
	public String health() {
		return "OK";
	}
}
