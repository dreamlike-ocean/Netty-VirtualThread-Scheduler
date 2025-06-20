package io.netty.loom;

import io.netty.util.internal.PlatformDependent;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public final class LoomSupport {
   private static final MethodHandle SCHEDULER;
   private static final VarHandle CARRIER_THREAD;
   private static final Throwable CUSTOM_SCHEDULER_FAILURE;
   private static final MethodHandle VT_SCHEDULER_GETTER_MH;

   static {
      Throwable error = null;
      MethodHandle scheduler;
      VarHandle carrierThread;
      MethodHandle vtSchedulerGetterMH;
      try {
         // this is required to override the default scheduler
         MethodHandles.Lookup lookup = MethodHandles.lookup();
         Field schedulerField = Class.forName("java.lang.ThreadBuilders$VirtualThreadBuilder")
                 .getDeclaredField("scheduler");
         schedulerField.setAccessible(true);
         scheduler = lookup.unreflectSetter(schedulerField);

         var builder = Thread.ofVirtual();
         scheduler.invoke(builder, new Executor() {
            @Override
            public void execute(Runnable command) {

            }
         });

         carrierThread = findVarHandle(Class.forName("java.lang.VirtualThread"),
                 "carrierThread", Thread.class);
         // try this once to ensure that we can access the carrier thread
         carrierThread.getVolatile(Thread.ofVirtual().start(new Runnable() {
            @Override
            public void run() {

            }
         }));

         Field threadSchedulerField = Class.forName("java.lang.VirtualThread")
                 .getDeclaredField("scheduler");
         threadSchedulerField.setAccessible(true);
         vtSchedulerGetterMH = lookup.unreflectGetter(threadSchedulerField)
                 .asType(MethodType.methodType(Executor.class, Thread.class));
      } catch (Throwable e) {
         scheduler = null;
         carrierThread = null;
         error = e;
         vtSchedulerGetterMH = null;
      }

      CUSTOM_SCHEDULER_FAILURE = error;
      SCHEDULER = scheduler;
      CARRIER_THREAD = carrierThread;
      VT_SCHEDULER_GETTER_MH = vtSchedulerGetterMH;
   }

   private static VarHandle findVarHandle(Class<?> declaringClass, String fieldName, Class<?> fieldType) {
      try {
         MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(declaringClass, MethodHandles.lookup());
         return lookup.findVarHandle(declaringClass, fieldName, fieldType);
      } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
         throw new RuntimeException("Cannot get VarHandle for " +
                 declaringClass.getSimpleName() + "." + fieldName, e);
      }
   }

   private LoomSupport() {
   }

   public static boolean isSupported() {
      return CUSTOM_SCHEDULER_FAILURE == null;
   }

   public static void checkSupported() {
      if (!isSupported()) {
         // print whatever error we have
         if (CUSTOM_SCHEDULER_FAILURE != null) {
            throw new UnsupportedOperationException("Custom scheduler is not supported", CUSTOM_SCHEDULER_FAILURE);
         }
      }
   }

   public static Thread getCarrierThread(Thread t) {
      checkSupported();
      if (!t.isVirtual()) {
         return t;
      }
      try {
         return (Thread) CARRIER_THREAD.getVolatile(t);
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
   }

   public static Executor getScheduler(Thread t) {
      try {
         return (Executor) VT_SCHEDULER_GETTER_MH.invokeExact(t);
      } catch (Throwable e) {
         PlatformDependent.throwException(e);
         throw new AssertionError();
      }
   }

   public static Thread.Builder.OfVirtual setVirtualThreadFactoryScheduler(Thread.Builder.OfVirtual builder,
                                                                           Executor vthreadScheduler) {
      checkSupported();
      try {
         SCHEDULER.invoke(builder, vthreadScheduler);
         return builder;
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
   }
}
