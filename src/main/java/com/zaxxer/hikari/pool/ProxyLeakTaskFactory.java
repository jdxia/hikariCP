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

package com.zaxxer.hikari.pool;

import java.util.concurrent.ScheduledExecutorService;

/**
 * A factory for {@link ProxyLeakTask} Runnables that are scheduled in the future to report leaks.
 *
 * @author Brett Wooldridge
 * @author Andreas Brenk
 *
 * 在处理连接泄露时使用到了工厂模式，只需要将连接实例 PoolEntry 传入工厂，即可提交连接泄露检测的延时任务。而所谓的链接泄露检测只是打印 1 次 WARN 日志
 */
class ProxyLeakTaskFactory
{
   private ScheduledExecutorService executorService;

   // 配置leakDetectionThreshold
   private long leakDetectionThreshold;

   ProxyLeakTaskFactory(final long leakDetectionThreshold, final ScheduledExecutorService executorService)
   {
      this.executorService = executorService;
      this.leakDetectionThreshold = leakDetectionThreshold;
   }

   // 1、传入连接对象
   ProxyLeakTask schedule(final PoolEntry poolEntry)
   {
      /**
       * 连接泄露检测时间等于 0 不生效
       * 如果配置leakDetectionThreshold，会按照leakDetectionThreshold定时执行ProxyLeakTask。否则返回ProxyLeakTask.NO_LEAK，是个空实现。
       *
       * 如果连接在leakDetectionThreshold时间内被归还（即调用了close()方法），系统会调用leakTask.cancel()取消定时任务，从而避免触发run()方法
       * 获取到连接之后使用之前的时间+使用连接的时间+使用之后还回连接之前的时间，超出了leakDetectionThreshold毫秒，就抛出检测到连接泄露的异常
       */
      return (leakDetectionThreshold == 0) ? ProxyLeakTask.NO_LEAK : scheduleNewTask(poolEntry);
   }

   void updateLeakDetectionThreshold(final long leakDetectionThreshold)
   {
      this.leakDetectionThreshold = leakDetectionThreshold;
   }

   // 2、提交延时任务
   private ProxyLeakTask scheduleNewTask(PoolEntry poolEntry) {
      /**
       * 看这个 ProxyLeakTask 类的run方法
       *
       * 构造函数也要看, 创建出了一个异常对象
       */
      var task = new ProxyLeakTask(poolEntry);
      task.schedule(executorService, leakDetectionThreshold);

      return task;
   }
}
