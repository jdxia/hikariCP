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

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.concurrent.Semaphore;

/**
 * This class implements a lock that can be used to suspend and resume the pool.  It
 * also provides a faux implementation that is used when the feature is disabled that
 * hopefully gets fully "optimized away" by the JIT.
 *
 * @author Brett Wooldridge
 * 专门用于连接池挂起和恢复，利用的是JUC的Semaphore
 * acquire：每次getConnection都会调用这个方法，如果当前连接池处于挂起状态，获取不到信号量将阻塞等待。
 * release：释放信号量。
 * suspend：阻塞等待获取所有信号量。
 * resume：释放所有信号量
 */
public class SuspendResumeLock
{
   // FAUX_LOCK是SuspendResumeLock的一个空实现，作者希望通过JIT optimized away 优化掉FAUX_LOCK相关的调用
   public static final SuspendResumeLock FAUX_LOCK = new SuspendResumeLock(false) {
      @Override
      public void acquire() {}

      @Override
      public void release() {}

      @Override
      public void suspend() {}

      @Override
      public void resume() {}
   };

   // 信号量最大许可
   private static final int MAX_PERMITS = 10000;

   // 信号量
   private final Semaphore acquisitionSemaphore;

   /**
    * Default constructor
    */
   public SuspendResumeLock()
   {
      this(true);
   }

   private SuspendResumeLock(final boolean createSemaphore)
   {
      acquisitionSemaphore = (createSemaphore ? new Semaphore(MAX_PERMITS, true) : null);
   }

   public void acquire() throws SQLException
   {
      // 先尝试获取信号量
      if (acquisitionSemaphore.tryAcquire()) {
         return;
      }

      // 尝试获取信号量失败，com.zaxxer.hikari.throwIfSuspended如果为true，直接抛出异常
      else if (Boolean.getBoolean("com.zaxxer.hikari.throwIfSuspended")) {
         throw new SQLTransientException("The pool is currently suspended and configured to throw exceptions upon acquisition");
      }

      // 尝试获取信号量失败，这里阻塞等待获取到新的许可
      acquisitionSemaphore.acquireUninterruptibly();
   }

   public void release()
   {
      acquisitionSemaphore.release();
   }

   public void suspend()
   {
      acquisitionSemaphore.acquireUninterruptibly(MAX_PERMITS);
   }

   public void resume()
   {
      acquisitionSemaphore.release(MAX_PERMITS);
   }
}
