# spring-example

Spring Boot MVC application running on Netty with the Netty VirtualThread Scheduler.

Uses [dsyer/dispatcher-servlet-container](https://github.com/dsyer/dispatcher-servlet-container)
to bridge Spring MVC's `DispatcherServlet` onto Netty, replacing Tomcat with our
`VirtualIoNativePollerEventLoopGroup` backed by epoll pinned pollers.

Blocking `@RestController` handlers run on virtual threads with carrier affinity.
No WebFlux, no `Mono`, no reactive — plain blocking Spring MVC code.

## Endpoints

| Path | Description |
|---|---|
| `GET /` | Blocking handler (50ms sleep), returns VT name + carrier info |
| `GET /parallel` | `StructuredTaskScope` fork/join — two tasks on the same carrier |
| `GET /health` | Quick health check |

## Prerequisites

- A Loom-enabled JDK (Java 27+, set `JAVA_HOME`)
- Maven 3.6+
- The `dispatcher` module installed locally (see below)

## Build

```bash
export JAVA_HOME=/path/to/loom/build/linux-x86_64-server-release/jdk

# install the dispatcher-servlet-container dependency
cd /tmp && git clone --depth 1 https://github.com/dsyer/dispatcher-servlet-container.git
cd dispatcher-servlet-container && mvn -B install -pl dispatcher -DskipTests
cd -

# build from repository root
mvn -DskipTests package
```

## Run (flat classpath — development)

```bash
cd spring-example
DEPS=$(mvn -B dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)
"$JAVA_HOME/bin/java" --enable-preview --enable-native-access=ALL-UNNAMED \
  -Djdk.virtualThreadScheduler.implClass=io.netty.loom.scheduler.NettyScheduler \
  -cp "target/classes:$DEPS" io.netty.loom.example.Main
```

## Run (fat JAR — production)

This exercises the classloader challenge: bootstrap + jctools are unpacked into
`META-INF/versions/27/` (MRJAR) so the system classloader finds them despite
Spring Boot's `LaunchedClassLoader`.

```bash
"$JAVA_HOME/bin/java" --enable-preview --enable-native-access=ALL-UNNAMED \
  -Djdk.virtualThreadScheduler.implClass=io.netty.loom.scheduler.NettyScheduler \
  -jar target/spring-example-1.0-SNAPSHOT.jar
```

## Smoke test

```bash
curl http://localhost:8080/
# HELLO from VirtualThread[#131]/runnable@Thread-2 on carrier Thread-2 (scheduler 1)

curl http://localhost:8080/parallel
# A from VirtualThread[#134]/runnable@Thread-4 | B from VirtualThread[#135]/runnable@Thread-4
```

Both tasks in `/parallel` run on the same carrier — structured concurrency with carrier affinity.
