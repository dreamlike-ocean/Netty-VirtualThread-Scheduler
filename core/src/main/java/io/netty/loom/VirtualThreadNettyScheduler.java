package io.netty.loom;

import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ManualIoEventLoop;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * 这里简要概括一下
 * 这里的目的是使用“IoEventLoop线程”运行VirtualThread，如果EventLoop为平台线程那么在平台线程vt和这个平台线程会触及同一个Lock/blockQueue的时候有概率发生死锁问题
 * 所以这里希望将EventLoop运行在vt上，而平台线程从EventLoop中剥离，不会直接接受任何运行的普通Runnable，只会运行虚拟线程的Runnable
 * 所以这里的实现思路是：
 * 1, 创建一个ioEventLoop虚拟线程，这个线程会运行ioHandler，同时获取这个vt中的continuation（runnable形式的），
 *    在对应的平台线程的run方法中运行这个continuation，同时他会根据当前负载/IO读取情况通过Thread::yield主动让出执行权，同时再此投递到平台线程中，等待下次调用
 * 2, 利用当前载体创建n个虚拟线程，即VirtualThreadNettyScheduler::toVTExecutor
 *
 * 原有的EventLoop上的定时任务时间源仍由IoHandle处理，即你看到的VirtualThreadNettyScheduler::runIOHandler中的blockingRunIoEventLoop调用
 * 对于jdk的selector其阻塞会让当前VT让出并不会阻塞当前载体线程，但是native的selector则不会
 * 为了使得Selector的jdk实现和native效果一致，你在代码里面还能看到park carrierThread的代码
 *
 * 调用顺序应该是
 * driverVirtualThreadAndIOEventLoop -> runIOHandler -> runExternalContinuations
 */
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
      // 确保第一个任务一定是runIOHandler的任务 只有拿到IO的continuation的时候才能提供外部运行
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
      // 拿到eventLoopContinuation 方便手操是否继续IO操作
      this.eventLoopContinuation = command;
      // we need to notify the event loop that we have a continuation
      // 可以对外提供服务了
      eventLoopContinuationAvailable.countDown();
      return command;
   }

   Executor toVTExecutor() {
      return Executors.newThreadPerTaskExecutor(vThreadFactory);
   }

}
