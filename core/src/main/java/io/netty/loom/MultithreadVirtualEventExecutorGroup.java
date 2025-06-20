package io.netty.loom;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

import io.netty.channel.EventLoop;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocal;


public class MultithreadVirtualEventExecutorGroup extends MultiThreadIoEventLoopGroup {

   private static final int RESUMED_CONTINUATIONS_EXPECTED_COUNT = Integer.getInteger("io.netty.loom.resumed.continuations", 1024);
   private IdentityHashMap<EventLoop, VirtualThreadNettyScheduler> schedulers;
   private IdentityHashMap<Thread, VirtualThreadNettyScheduler> schedulersThreadMapping;

   private final FastThreadLocal<VirtualThreadNettyScheduler> v_thread_factory = new FastThreadLocal<>() {
      @Override
      protected VirtualThreadNettyScheduler initialValue() {
         return schedulersThreadMapping.get(Thread.currentThread());
      }
   };

   public MultithreadVirtualEventExecutorGroup(int nThreads, IoHandlerFactory ioHandlerFactory) {
      this(nThreads, new DefaultThreadFactory(MultithreadVirtualEventExecutorGroup.class, 10), ioHandlerFactory);
   }

   public MultithreadVirtualEventExecutorGroup(int nThreads, ThreadFactory threadFactory, IoHandlerFactory ioHandlerFactory) {
      super(nThreads, (Executor) command -> {
         throw new UnsupportedOperationException("this executor is not supposed to be used");
      }, ioHandlerFactory, threadFactory);
   }

   public ThreadFactory vThreadFactory() {
      return v_thread_factory.get().virtualThreadFactory();
   }

   @Override
   protected IoEventLoop newChild(Executor executor, IoHandlerFactory ioHandlerFactory, Object... args) {
      // 从构造器传入的 受限于super的编译器限制
      // 只能这样传递
      ThreadFactory threadFactory = ((ThreadFactory) args[0]);
      var customScheduler = new VirtualThreadNettyScheduler(this, threadFactory, ioHandlerFactory, RESUMED_CONTINUATIONS_EXPECTED_COUNT);
      if (schedulers == null) {
         schedulers = new IdentityHashMap<>();
         schedulersThreadMapping = new IdentityHashMap<>();
      }
      schedulers.put(customScheduler.ioEventLoop(), customScheduler);
      schedulersThreadMapping.put(customScheduler.eventLoopThread(), customScheduler);
      return customScheduler.ioEventLoop();
   }

   public Map<EventLoop, Executor> VTExecutors() {
      HashMap<EventLoop, Executor> vtExecutors = new HashMap<>(schedulers.size());
      schedulers.forEach((eventLoop, scheduler) -> vtExecutors.put(eventLoop, scheduler.toVTExecutor()));
      return vtExecutors;
   }
}
