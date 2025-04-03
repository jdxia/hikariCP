/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zaxxer.hikari.util;

import com.zaxxer.hikari.pool.HikariPool;
import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 *
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * @author Brett Wooldridge
 *
 * @param <T> the templated type to store in the bag
 *
 * 为连接池设计的一个并发类
 * 简而言之，Hikari 通过 CopyOnWriteArrayList + State（状态） + CAS 来避免了上锁
 * ConcurrentBag 才是真正的连接池，也是 Hikari “零开销”的奥秘所在
 * 本质就是通过标记来实现的
 * 性能优化: 无锁, 提供了 threadLocal的缓存和队列窃取
 */
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);

   /**
    * 保存所有元素, 通过CopyOnWriteArrayList + State + cas 来避免了上锁
    *
    * 使用CopyOnWriteArrayList，写操作加锁并复制底层数据，适用于读多写少的场景。
    * HikariCP作者建议连接池最大连接数与最小连接数保持一致，这可能也是其中一个原因。
    * 如果连接池配置为弹性容量，遇到突发流量，sharedList扩张就导致CopyOnWriteArrayList加锁并做数组拷贝；
    * 流量过后，sharedList收缩也会导致加锁和数组拷贝
    */
   private final CopyOnWriteArrayList<T> sharedList;

   /**
    * 是否开启ThreadLocal保存元素
    * 对于threadList中的元素是否使用 WeakReference 包装，默认否
    *
    * 弱引用对象不会阻止垃圾回收器回收所引用的对象
    * 当JVM内存不足时，如果对象只有弱引用指向它，这个对象会被垃圾回收
    */
   private final boolean weakThreadLocals;

   /**
    * 当前线程持有的元素
    * 可以认为sharedList包含了所有的threadList里的元素
    *
    * 归还的时候缓存空闲连接到 ThreadLocal：requite()、borrow()
    * 里面的list 是 FastList
    */
   private final ThreadLocal<List<Object>> threadList;

   // IBagStateListener（HikariPool）
   private final IBagStateListener listener;

   /**
    * 等待者数量
    * 等待获取连接的线程数：调 borrow() 方法+1，调完-1
    */
   private final AtomicInteger waiters;

   // 连接池关闭标识
   private volatile boolean closed;

   /**
    * 交接队列
    * 队列大小为0的阻塞队列：生产者消费者模式
    * 主要用到SynchronousQueue的两个方法offer（当没有线程获取走offer的元素时返回false）和poll(timeout,unit)（指定时间内没有获取到元素时返回null）
    */
   private final SynchronousQueue<T> handoffQueue;

   // ConcurrentBag中的元素
   public interface IConcurrentBagEntry
   {
      // 未使用。可以被借走
      int STATE_NOT_IN_USE = 0;

      // 正在使用
      int STATE_IN_USE = 1;

      // 被移除，只有调用remove方法时会CAS改变为这个状态，修改成功后会从容器中被移除
      int STATE_REMOVED = -1;

      // 被保留，不能被使用。往往是移除前执行保留操作
      int STATE_RESERVED = -2;

      boolean compareAndSet(int expectState, int newState);
      void setState(int newState);
      int getState();
   }

   // 用于通知外部，ConcurrentBag需要添加元素了
   public interface IBagStateListener
   {
      // waiting表示需要添加几个元素
      void addBagItem(int waiting);
   }

   /**
    * Construct a ConcurrentBag with the specified listener.
    *
    * @param listener the IBagStateListener to attach to this bag
    */
   public ConcurrentBag(final IBagStateListener listener)
   {
      // IBagStateListener必须传入, 这个 listener 就是 HikariPool
      this.listener = listener;

      /**
       * threadList是否使用WeakReference保存元素
       * 是不同的类加载器启动的线程, 线程上下文加载器不是 jdk app classloader 这时候, threadlocal里面就回收不了
       *
       * 既想要持有对象的引用，又不想参与对象是否可以回收的决定，都可以这么写
       * 弱引用：弱引用对象不会阻止垃圾回收器回收所引用的对象
       */
      this.weakThreadLocals = useWeakThreadLocals();

      // 交接队列，fair=true
      this.handoffQueue = new SynchronousQueue<>(true);

      // 等待线程数量
      this.waiters = new AtomicInteger();

      // 保存容器内所有元素
      this.sharedList = new CopyOnWriteArrayList<>();
      if (weakThreadLocals) {
         // 如果使用WeakReference，用ArrayList
         this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
      }
      else {
         // 否则使用FastList，默认
         this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
      }
   }

   /**
    * The method will borrow a BagEntry from the bag, blocking for the
    * specified timeout if none are available.
    *
    * @param timeout how long to wait before giving up, in units of unit
    * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
    * @return a borrowed instance from the bag or null if a timeout occurs
    * @throws InterruptedException if interrupted while waiting
    *
    * 从bag中借出元素，如果没有可以获取的元素，会阻塞指定时长
    */
   public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException
   {
      // Try the thread-local list first
      // 先从threadList ThreadLocal获取
      final var list = threadList.get();
      for (int i = list.size() - 1; i >= 0; i--) {
         // 从尾部读取：后缓存的优先用，细节！
         final var entry = list.remove(i);

         /**
          * 是不同的类加载器启动的线程, 线程上下文加载器不是 jdk app classloader 这时候, threadlocal里面就回收不了
          */
         @SuppressWarnings("unchecked")
         final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;

         /**
          * CAS修改元素状态为使用中
          * 因为元素可能被其他线程偷取，所以要cas修改状态
          */
         if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
            return bagEntry;
         }
      }

      // Otherwise, scan the shared list ... then poll the handoff queue
      // 如果本地缓存获取不到，从 shardList 连接池中获取，等待连接数+1, 增加等待线程数量
      final int waiting = waiters.incrementAndGet();
      try {
         // 从共享列表里获取
         for (T bagEntry : sharedList) {

            // CAS修改元素状态为使用中
            if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               // If we may have stolen another waiter's connection, request another bag add.
               /**
                * 如果不止有当前线程等待，可能偷取了别人的元素，通知外部放入元素
                * 并发情况下，保证能够及时补充连接
                */
               if (waiting > 1) {

                  /**
                   * 不能省略这个
                   * 这会导致其他线程获取元素失败。
                   * ThreadA因为从shareList获取元素失败，通知Listener往ConcurrentBag放入元素，
                   * 但是外部元素刚被放入shareList就被窃取了，导致ThreadA从交接队列获取元素失败，最终导致超时。
                   */
                  listener.addBagItem(waiting - 1);
               }
               return bagEntry;
            }
         }

         /**
          * 通知外部添加元素, 创建链接
          * 如果 shardList 连接池中也没获得连接，提交添加连接的异步任务，然后再从 handoffQueue 阻塞获取。
          *
          * 这里触发的是 {@link HikariPool#addBagItem(int)} 它会创建实际数据库连接，将实际Connection封装到PoolEntry中，再将PoolEntry放入ConcurrentBag中
          */
         listener.addBagItem(waiting);

         // 超时时间
         timeout = timeUnit.toNanos(timeout);
         do {
            final var start = currentTime();

            // 从handoffQueue交接队列等待获取
            final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS);

            /**
             * CAS修改元素状态为使用中
             *
             * 这里会出现三种情况，
             * 1.超时，返回null
             * 2.获取到元素，但状态为正在使用，继续执行
             * 3.获取到元素，元素状态未未使用，修改未使用并返回
             */
            if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               return bagEntry;
            }

            // timeout -= 本次循环消耗时间
            timeout -= elapsedNanos(start);
         } while (timeout > 10_000);

         // 超时返回null
         return null;
      }
      finally {
         // 等待连接数减 1, 减少等待者数量
         waiters.decrementAndGet();
      }
   }

   /**
    * This method will return a borrowed object to the bag.  Objects
    * that are borrowed from the bag but never "requited" will result
    * in a memory leak.
    *
    * @param bagEntry the value to return to the bag
    * @throws NullPointerException if value is null
    * @throws IllegalStateException if the bagEntry was not borrowed from the bag
    *
    * requite方法归还从bag借出的元素。如果借出的元素不做归还操作，会导致内存泄露。
    */
   public void requite(final T bagEntry)
   {
      // 设置元素状态为未使用，设置完成后可能会被其他线程抢走
      bagEntry.setState(STATE_NOT_IN_USE);

      /**
       * 如果有等待者，尝试放入交接队列
       * 如果有线程正在获取链接，则优先通过 handoffQueue 阻塞队列归还给其他线程使用
       */
      for (var i = 0; waiters.get() > 0; i++) {
         /**
          * 再次判断元素状态，因为可能被其他线程抢走，如果不是未使用状态直接结束
          * 尝试放入交接队列，如果放入成功直接结束
          */
         if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
            return;
         }
         // 如果循环了255次，把当前线程挂起一会
         else if ((i & 0xff) == 0xff) {
            // 每遍历 255 个休眠 10 微妙
            parkNanos(MICROSECONDS.toNanos(10));
         }
         else {
            // 线程让步
            Thread.yield();
         }
      }

      // 如果没有等待者或者循环了一圈没能放入交接队列，则放入ThreadLocal
      // 不动 sharedList, 因为链接一创建就放到 sharedList里面, shardList里面有所有链接的引用, threadLocal只是拿链接的快捷方式
      // 这怎么会，所有的连接sl里都持有引用
      final var threadLocalList = threadList.get();

      // 大于五十就不塞 threadLocalList 了
      if (threadLocalList.size() < 50) {
         threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
      }
   }

   /**
    * Add a new object to the bag for others to borrow.
    *
    * @param bagEntry an object to add to the bag
    */
   public void add(final T bagEntry)
   {
      // 如果容器关闭，抛出IllegalStateException
      if (closed) {
         LOGGER.info("ConcurrentBag has been closed, ignoring add()");
         throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
      }

      // 放入sharedList，此时其他线程已经可以获取这个元素了
      // sharedList只有所有链接的引用
      sharedList.add(bagEntry);

      // spin until a thread takes it or none are waiting
      /**
       * 自旋直到有线程获取它或没有线程在等待, 持续尝试将元素放入交接队列
       * 如果有线程等待获取连接，循环通过 handoffQueue 提交连接
       *
       * waiters.get() > 0：需要有正在等待获取元素的线程，才会循环
       * bagEntry.getState() == STATE_NOT_IN_USE：因为元素已经放入shareList了，可能被其他线程改变状态，需要判断当前元素仍然是未使用状态
       * !handoffQueue.offer(bagEntry)：尝试放入交接队列，如果失败继续循环
       */
      while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {
         // 当前线程主动放弃cpu执行，回到就绪状态
         Thread.yield();
      }
   }

   /**
    * Remove a value from the bag.  This method should only be called
    * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
    *
    * @param bagEntry the value to remove
    * @return true if the entry was removed, false otherwise
    * @throws IllegalStateException if an attempt is made to remove an object
    *         from the bag that was not borrowed or reserved first
    *
    * 从bag中移除元素。只有在调用完borrow或reserve后才能调用
    */
   public boolean remove(final T bagEntry)
   {
      /**
       * cas改变状态，前置状态只能是 STATE_IN_USE 或 STATE_RESERVED
       * 使用 CAS 将连接置为 STATE_REMOVED 状态
       */
      if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
         return false;
      }

      // 先从sharedList移除
      final boolean removed = sharedList.remove(bagEntry);
      if (!removed && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
      }

      // 再从ThreadLocal里移除
      threadList.get().remove(bagEntry);

      return removed;
   }

   /**
    * Close the bag to further adds.
    */
   @Override
   public void close()
   {
      closed = true;
   }

   /**
    * This method provides a "snapshot" in time of the BagEntry
    * items in the bag in the specified state.  It does not "lock"
    * or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in list before performing any action on them.
    *
    * @param state one of the {@link IConcurrentBagEntry} states
    * @return a possibly empty list of objects having the state specified
    */
   public List<T> values(final int state)
   {
      final var list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
      Collections.reverse(list);
      return list;
   }

   /**
    * This method provides a "snapshot" in time of the bag items.  It
    * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in the list, or understand the concurrency implications of
    * modifying items, before performing any action on them.
    *
    * @return a possibly empty list of (all) bag items
    */
   @SuppressWarnings("unchecked")
   public List<T> values()
   {
      return (List<T>) sharedList.clone();
   }

   /**
    * The method is used to make an item in the bag "unavailable" for
    * borrowing.  It is primarily used when wanting to operate on items
    * returned by the <code>values(int)</code> method.  Items that are
    * reserved can be removed from the bag via <code>remove(T)</code>
    * without the need to unreserve them.  Items that are not removed
    * from the bag can be make available for borrowing again by calling
    * the <code>unreserve(T)</code> method.
    *
    * @param bagEntry the item to reserve
    * @return true if the item was able to be reserved, false otherwise
    *
    * 保留状态可以先让元素状态变为不能借取之后，做一些逻辑操作，最后调用remove方法真正移除
    */
   public boolean reserve(final T bagEntry)
   {
      // cas修改状态为保留
      return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
   }

   /**
    * This method is used to make an item reserved via <code>reserve(T)</code>
    * available again for borrowing.
    *
    * @param bagEntry the item to unreserve
    */
   @SuppressWarnings("SpellCheckingInspection")
   public void unreserve(final T bagEntry)
   {
      // cas修改状态为未使用
      if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
         // spin until a thread takes it or none are waiting
         // 如果有等待获取元素的线程，尝试放入交接队列
         while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
            Thread.yield();
         }
      }
      else {
         LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
      }
   }

   /**
    * Get the number of threads pending (waiting) for an item from the
    * bag to become available.
    *
    * @return the number of threads waiting for items from the bag
    */
   public int getWaitingThreadCount()
   {
      return waiters.get();
   }

   /**
    * Get a count of the number of items in the specified state at the time of this call.
    *
    * @param state the state of the items to count
    * @return a count of how many items in the bag are in the specified state
    */
   public int getCount(final int state)
   {
      var count = 0;
      for (var e : sharedList) {
         if (e.getState() == state) {
            count++;
         }
      }
      return count;
   }

   public int[] getStateCounts()
   {
      final var states = new int[6];
      for (var e : sharedList) {
         ++states[e.getState()];
      }
      states[4] = sharedList.size();
      states[5] = waiters.get();

      return states;
   }

   /**
    * Get the total number of items in the bag.
    *
    * @return the number of items in the bag
    */
   public int size()
   {
      return sharedList.size();
   }

   public void dumpState()
   {
      sharedList.forEach(entry -> LOGGER.info(entry.toString()));
   }

   /**
    * Determine whether to use WeakReferences based on whether there is a
    * custom ClassLoader implementation sitting between this class and the
    * System ClassLoader.
    *
    * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
    */
   private boolean useWeakThreadLocals()
   {
      try {
         // 如果系统变量（-D参数或环境变量）有配置com.zaxxer.hikari.useWeakReferences，走系统变量配置
      	// 这个没有标注在文档里，因为一般不建议修改
         if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
            return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
         }

         // 如果当前类加载器和系统类加载器不一致，返回true
         return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
      }
      catch (SecurityException se) {
         return true;
      }
   }
}
