package com.zaxxer.hikari;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDemo {

   private static final String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/app?useUnicode=true&characterEncoding=utf-8&useSSL=false&verifyServerCertificate=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true";

   /**
    * 使用java11, idea里面设置运行前不编译这个
    * 每次运行前需要 mvn clean compile
    * <p>
    * compile 会执行 JavassistProxyFactory 这个类, 生成一些类
    */
   public static void main(String[] args) throws Exception {

      setRootLoggerTrace();

      String sql = "select * from gs_order_sub limit 3";
      // 连接池启动
      HikariDataSource dataSource = getDataSource();

      // try 括号内先声明的资源后关闭，后声明的资源先关闭
      try (Connection connection = dataSource.getConnection();
           // 预编译sql, hikaricp里面会追踪这个 statement , connection 被close的时候会清理这个
           PreparedStatement preparedStatement = connection.prepareStatement(sql);
           ResultSet resultSet = preparedStatement.executeQuery()) {

         while (resultSet.next()) {
            System.out.println(resultSet.getMetaData().getCatalogName(1) + ": " + resultSet.getString(1));
         }
      }
      SECONDS.sleep(100);

      // 关闭数据源
      dataSource.close();
   }

   private static HikariDataSource getDataSource() {
      /**
       * 配置可以看下面的链接
       * https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby
       *
       * 所有的时间都是毫秒单位
       */
      HikariConfig hikariConfig = new HikariConfig();

      hikariConfig.setUsername("root");
      hikariConfig.setPassword("hello.world123");
      hikariConfig.setJdbcUrl(jdbcUrl);

      /**
       * 表示数据源的数据库连接的最大数量，包括可用连接和已用连接，如果不配，默认是10支持 JMX 动态修改
       */
      hikariConfig.setMaximumPoolSize(5);

      /**
       * 连接泄露检测
       */
      hikariConfig.setLeakDetectionThreshold(SECONDS.toMillis(10));

      /**
       * 控制数据源的最小空闲连接数，也就是当前数据源如果连接数小于等于该配置，那么就算一个连接空闲时间达到了清理的条件，这个连接也不会被清理。
       * 不同于另外两款数据源，对于HikariCP这个数据源来说，我建议把minimumIdle和maximumPoolSize配置为相同的值，这样可以保持数据源的连接数相对稳定，以达到更机制的速度。
       * 支持 JMX 动态修改
       */
      hikariConfig.setMinimumIdle(2);

      /**
       * 该配置项控制连接的物理存活时间，严格意义来讲，这不是一个保活配置，但是却能达到和保活差不多的效果。
       * 如果配置了maxLifetime则每个连接在创建出来时会被添加一个定时任务，
       * 大约在maxLifetime时间时会触发定时任务，触发定时任务时如果连接没有被使用则直接销毁连接，
       * 如果连接有被使用则标记连接为软销毁，被标记为 软销毁的连接 在下一次被获取时 就会被物理销毁。默认是1800s，最小可设置为30s，
       * 小于最小值时会被重置为默认值，建议配置为一个略小于T的值（T表示连接空闲多久时会被无感知的断开）
       *
       * 作者的意思很明确，他认为一个连接即使一直能用，也不应该一直存在下去，应该定时的关闭（哪怕一天关一次），好让数据库服务器那边清理掉一些浪费的资源。
       * 所以这才是maxLifetime最根本存在的原因。
       */
      hikariConfig.setMaxLifetime(SECONDS.toMillis(60));

      hikariConfig.setAutoCommit(true);

      /**
       * 数据源拿着即将借出去的连接与数据库服务端做一次交互，如果配了connectionTestQuery，那么就是执行一次配置的SQL语句，如果connectionTestQuery没有配，那么此时就是Ping一次数据库服务端。
       * HikariCP数据源的connectionTestQuery默认是null，所以请一定要配上connectionTestQuery
       */
      hikariConfig.setConnectionTestQuery("SELECT 1");

      // 当我从池中借出连接时，愿意等待多长时间。如果超时，将抛出 SQLException
      // 默认 30000 ms，最小值 250 ms。支持 JMX 动态修改
      hikariConfig.setConnectionTimeout(MINUTES.toMillis(10));

      /**
       * 无论是通过Ping还是通过执行SQL来校验连接，肯定是需要一个超时时间的，
       * validationTimeout就是决定这个超时时间，默认是5000ms，最小可以设置为250ms，小于最小值会被设置为默认值
       */
      hikariConfig.setValidationTimeout(SECONDS.toMillis(5));

      /**
       * 控制非核心连接空闲达到多久会被销毁。
       * 这里说的非核心连接就是数量大于minimumIdle小于等于maximumPoolSize的这一部分连接，后台会有线程每隔30s就判断一次非核心连接是否空闲达到idleTimeout，如果达到就销毁这个连接。
       * 该配置默认是600s，最小可以配置为10s，如果配置小于10s则会被重置为默认的600s。这个配置仅在minimumIdle < maximumPoolSize时生效
       *
       * maxLifetime如果过长，且idleTimeout没有时间限制时，会导致连接数很大，空闲连接一直得不到释放，严重挤占资源，容易引起连接数不够的问题
       */
      hikariConfig.setIdleTimeout(SECONDS.toMillis(10));

      /**
       * HikariCP数据源连接保活的最重要配置项。配置了该参数后，每个连接在创建出来时会被添加一个周期定时任务，
       * 每隔keepaliveTime的时间就会对连接做一次保活，如果定时任务触发时连接正在被使用，则当次保活取消。
       * 默认是0表示不保活，建议一定要配置一个小于T的值（T表示连接空闲多久时会被无感知的断开），如果配置为一个小于30s的值，则保活功能也会关闭
       */
      hikariConfig.setKeepaliveTime(SECONDS.toMillis(30));

      hikariConfig.setPoolName("TestDemo-pool");

      hikariConfig.setReadOnly(false);

      // MBean监控
      hikariConfig.setRegisterMbeans(true);

      hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");

      /**
       * 设置metric注册器 每3秒打印一次, 对应的指标在 com.zaxxer.hikari.metrics.PoolStats 中
       * spring boot 是 HikariDataSourceMetricsConfiguration
       */
      LoggingMeterRegistry loggingMeterRegistry = new LoggingMeterRegistry(new LoggingRegistryConfig() {
         @Override
         public String get(String key) {
            return null;
         }

         @Override
         public Duration step() {
            return Duration.ofSeconds(3);
         }
      }, Clock.SYSTEM);
      hikariConfig.setMetricRegistry(loggingMeterRegistry);

      /**
       * Statement Cache 不提供, hikaricp的readMe里面写了
       * 许多连接池，包括 Apache DBCP、c3p0 等，都提供 PreparedStatement 缓存。HikariCP 则不提供
       *
       * 在连接池层， PreparedStatements 只能按连接缓存。如果你的应用程序有 250 个常用查询和一个包含 20 个连接的池，
       * 你要求数据库保留 5000 个查询执行计划 —— 同样，池也必须缓存这么多 PreparedStatements 及其相关的对象图。并且需要java内存
       *
       * 大多数主要数据库的 JDBC 驱动程序已经具有可配置的 Statement 缓存，包括 pgsql、Oracle、Derby、MySQL、DB2 等。
       * JDBC 驱动程序处于利用数据库特定功能的独特位置，几乎所有缓存实现都能够跨连接共享执行计划。
       * 这意味着，内存中不是 5000 个语句及其关联的执行计划，而是 250 个常用查询在数据库中仅生成 250 个执行计划。
       * 聪明的实现甚至不会在驱动程序级别保留 PreparedStatement 对象在内存中，而是仅将新实例附加到现有的计划 ID 上。
       */

      /**
       * 关于Sql日志记录和慢日志
       * 可以看这个Issue: https://github.com/brettwooldridge/HikariCP/issues/57#issuecomment-354647631
       * 作者是不愿意在连接池层去做这种监控的事情的，应为会大大降低其性能。
       */

      /**
       * 建议用 hikariConfig 来创建 连接池, 这样创建出来的 fastPathPool 是final修饰的
       */
      return new HikariDataSource(hikariConfig);
   }


   private static void setRootLoggerTrace() {
      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      Configuration config = ctx.getConfiguration();
      LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
      loggerConfig.setLevel(Level.TRACE);
      ctx.updateLoggers();
   }

}
