package io.netty.loom;

import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ManualIoEventLoop;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class VirtualThreadNettyScheduler implements Executor {

   private static final long MAX_WAIT_TASKS_NS = TimeUnit.HOURS.toNanos(1);
   private static final long MAX_RUN_NS = TimeUnit.MICROSECONDS.toNanos(Integer.getInteger("io.netty.loom.run.us", 1));

   private final MpscUnboundedArrayQueue<Runnable> externalContinuations;
   private final ManualIoEventLoop ioEventLoop;
   private final Thread eventLoopThread;
   private final Thread carrierThread;
   private volatile Thread parkedCarrierThread;
   private volatile Runnable eventLoopContinuation;
   private volatile boolean submittedEventLoopContinuation;
   private final CountDownLatch eventLoopContinuationAvailable;
   private final ThreadFactory vThreadFactory;
   private final AtomicBoolean running;

   public VirtualThreadNettyScheduler(IoEventLoopGroup parent, ThreadFactory threadFactory, IoHandlerFactory ioHandlerFactory, int resumedContinuationsExpectedCount) {
      this.running = new AtomicBoolean(false);
      this.externalContinuations = new MpscUnboundedArrayQueue<>(resumedContinuationsExpectedCount);
      this.carrierThread = threadFactory.newThread(this::driverVirtualThreadAndIOEventLoop);
      var builder = LoomSupport.setVirtualThreadFactoryScheduler(Thread.ofVirtual(), this);
      this.vThreadFactory = builder
              .name(carrierThread.getName() + "-VirtualThreadWorker-", 0)
              .factory();
      this.eventLoopThread = builder.unstarted(
              // 为了诸如adaptive Allocator之类FastThreadLocalThread优化
              () -> FastThreadLocalThread.runWithFastThreadLocal(this::runIOHandler));
      this.eventLoopThread.setName(carrierThread.getName() + "-IOHandleVirtualThread");
      this.ioEventLoop = new ManualIoEventLoop(parent, eventLoopThread,
              ioExecutor -> new AwakeAwareIoHandler(running, ioHandlerFactory.newHandler(ioExecutor)));
      // we can start the carrier only after all the fields are initialized
      eventLoopContinuationAvailable = new CountDownLatch(1);
      carrierThread.start();
      // 确保第一个任务一定是runIOHandler的任务
      try {
         eventLoopContinuationAvailable.await();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   public ThreadFactory virtualThreadFactory() {
      return vThreadFactory;
   }

   public Thread eventLoopThread() {
      return eventLoopThread;
   }

   public ManualIoEventLoop ioEventLoop() {
      return ioEventLoop;
   }

   private void runIOHandler() {
      // 在单线程vt上跑的ioHandler
      running.set(true);
      assert ioEventLoop.inEventLoop(Thread.currentThread()) && Thread.currentThread().isVirtual();
      boolean canBlock = false;
      while (!ioEventLoop.isShuttingDown()) {
         if (canBlock) {
            // 如果是io_uring之类会让CarrierThread阻塞在这里 这也是eventloop的定时器时间来源
            // 如果在pin的时候需要运行vt/任务则需要唤醒ManualIoEventLoop
            canBlock = blockingRunIoEventLoop();
         } else {
            canBlock = ioEventLoop.runNow(MAX_RUN_NS) == 0;
         }
         // 让出vt的carrierThread执行权限 请上一步中产生的vt或者任务可以执行
         // vt yield本质是重新调用executor::execute放在队尾
         Thread.yield();
         // try running leftover write tasks before checking for I/O tasks
         canBlock &= ioEventLoop.runNonBlockingTasks(MAX_RUN_NS) == 0;
         Thread.yield();
      }
      // we are shutting down, it shouldn't take long so let's spin a bit :P
      while (!ioEventLoop.isTerminated()) {
         ioEventLoop.runNow();
      }
   }

   private boolean blockingRunIoEventLoop() {
      // try to go to sleep waiting for I/O tasks
      running.set(false);
      // StoreLoad barrier: see https://www.scylladb.com/2018/02/15/memory-barriers-seastar-linux/
      try {
         if (!canBlock()) {
            return false;
         }
         return ioEventLoop.run(MAX_WAIT_TASKS_NS, MAX_RUN_NS) == 0;
      } finally{
         running.set(true);
      }
   }

   // 这个函数就是载体线程上运行的runnable
   // 这里的函数内部跑在CarrierThread上面
   private void driverVirtualThreadAndIOEventLoop() {
      var eventLoop = this.ioEventLoop;
      // 启动跑IoHandle的VT线程 这里就是做到了用虚拟线程跑eventLoop
      // 也就是execute函数第一次被调用的地方 eventLoopContinuation 就是这样来的
      // eventLoopThread.start(); -> this.executor
      eventLoopThread.start();
      // eventLoopContinuation会在execute方法里面赋值
      // 即 eventLoopThread.start(); 后就会赋值
      var eventLoopContinuation = this.eventLoopContinuation;
      assert eventLoopContinuation != null && eventLoopContinuationAvailable.getCount() == 0;
      // we keep on running until the event loop is shutting-down
      while (!eventLoop.isTerminated()) {
         // 跑vt任务
         int count = runExternalContinuations(MAX_RUN_NS);
         // eventLoopThread.start()会投递一次
         // runIOHandler中的Thread.yield也会投递一次
         if (submittedEventLoopContinuation) {
            // 跑IO任务 以及随之而来的ChannelPipeline回调
            submittedEventLoopContinuation = false;
            eventLoopContinuation.run();
         } else if (count == 0) {
            parkedCarrierThread = carrierThread;
            if (canBlock()) {
               LockSupport.park();
            }
            parkedCarrierThread = null;
         }
      }
      while (!canBlock()) {
         // we still have continuations to run, let's run them
         runExternalContinuations(MAX_RUN_NS);
         if (submittedEventLoopContinuation) {
            submittedEventLoopContinuation = false;
            eventLoopContinuation.run();
         }
      }
   }

   private boolean canBlock() {
      return externalContinuations.isEmpty() && !submittedEventLoopContinuation;
   }

   private int runExternalContinuations(long deadlineNs) {
      assert eventLoopContinuation != null;
      final long startDrainingNs = System.nanoTime();
      var ready = this.externalContinuations;
      int runContinuations = 0;
      for (; ; ) {
         var continuation = ready.poll();
         if (continuation == null) {
            break;
         }
         continuation.run();
         runContinuations++;
         long elapsedNs = System.nanoTime() - startDrainingNs;
         if (elapsedNs >= deadlineNs) {
            return runContinuations;
         }
      }
      return runContinuations;
   }

   @Override
   public void execute(Runnable command) {
      if (ioEventLoop.isTerminated()) {
         throw new RejectedExecutionException("event loop is shutting down");
      }

      Runnable eventLoopContinuation = this.eventLoopContinuation;
      if (eventLoopContinuation == null) {
         eventLoopContinuation = setEventLoopContinuation(command);
      }
      // 跑IoHandle的不要放进externalContinuations里面
      // 由driverVirtualThreadAndIOEventLoop拿到eventLoopContinuation进行run
      if (eventLoopContinuation == command) {
         submittedEventLoopContinuation = true;
      } else {
         // 当作vt调度器来的各种vt内部的continuation
         externalContinuations.offer(command);
      }
      Thread currentThread = Thread.currentThread();
      // 如果当前是虚拟线程且跑在当前调度器上 那么不需要唤醒
      // 如果当前就是调度器线程 那么也不需要唤醒
      if (!isCurrentVTRunOnScheduler(currentThread) && !ioEventLoop.inEventLoop(currentThread)) {
         ioEventLoop.wakeup();
         LockSupport.unpark(parkedCarrierThread);
      }
   }

   private boolean isCurrentVTRunOnScheduler(Thread thread) {
      return thread.isVirtual() && LoomSupport.getScheduler(thread) == this;
   }

   private Runnable setEventLoopContinuation(Runnable command) {
      // this is the first command, we need to set the continuation
      this.eventLoopContinuation = command;
      // we need to notify the event loop that we have a continuation
      eventLoopContinuationAvailable.countDown();
      return command;
   }

   Executor toVTExecutor() {
      return Executors.newThreadPerTaskExecutor(vThreadFactory);
   }

}
