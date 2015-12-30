package io.snappydata.dunit.cluster

import java.io.File
import java.util.Properties

import scala.collection.JavaConverters._

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.{FabricService, TestUtil}
import dunit.{DistributedTestBase, Host, SerializableRunnable}
import io.snappydata.{Locator, Server, ServiceManager}
import org.slf4j.LoggerFactory

import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Base class for tests using Snappy ClusterManager. New utility methods
 * would need to be added as and when corresponding snappy code gets added.
 *
 * @author hemant
 */
class ClusterManagerTestBase(s: String) extends DistributedTestBase(s) {

  import ClusterManagerTestBase._

  val bootProps: Properties = new Properties()
  bootProps.setProperty("log-file", "snappyStore.log")
  bootProps.setProperty("log-level", "config")
  // Easier to switch ON traces. thats why added this.
  // bootProps.setProperty("gemfirexd.debug.true", "QueryDistribution,TraceExecution,TraceActivation")
  bootProps.setProperty("statistic-archive-file", "snappyStore.gfs")

  val host = Host.getHost(0)
  val vm0 = host.getVM(0)
  val vm1 = host.getVM(1)
  val vm2 = host.getVM(2)
  val vm3 = host.getVM(3)

  final def locatorPort: Int = DistributedTestBase.getDUnitLocatorPort

  protected final def startArgs =
    Array(locatorPort, bootProps).asInstanceOf[Array[AnyRef]]

  val locatorNetPort: Int = 0
  val locatorNetProps = new Properties()

  // SparkContext is initialized on the lead node and hence,
  // this can be used only by jobs running on Lead node
  def sc: SparkContext = SnappyContext.globalSparkContext

  override def setUp(): Unit = {
    val testName = getName
    val testClass = getClass
    // bootProps.setProperty(Attribute.SYS_PERSISTENT_DIR, s)
    TestUtil.currentTest = testName
    TestUtil.currentTestClass = getTestClass
    TestUtil.skipDefaultPartitioned = true
    TestUtil.doCommonSetup(bootProps)
    GemFireXDUtils.IS_TEST_MODE = true

    val locPort = locatorPort
    val locNetPort = locatorNetPort
    val locNetProps = locatorNetProps
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        val loc: Locator = ServiceManager.getLocatorInstance

        if (loc.status != FabricService.State.RUNNING) {
          loc.start("localhost", locPort, locNetProps)
        }
        if (locNetPort > 0) {
          loc.startNetworkServer("localhost", locNetPort, locNetProps)
        }
        assert(loc.status == FabricService.State.RUNNING)

        val logger = LoggerFactory.getLogger(getClass)
        logger.info("\n\n\n  STARTING TEST " + testClass.getName + '.' +
            testName + "\n\n")
      }
    })

    val nodeProps = bootProps
    val startNode = new SerializableRunnable() {
      override def run(): Unit = {
        val node = ServiceManager.currentFabricServiceInstance
        if (node == null || node.status != FabricService.State.RUNNING) {
          startSnappyServer(locPort, nodeProps)
        }
        assert(ServiceManager.currentFabricServiceInstance.status ==
            FabricService.State.RUNNING)

        val logger = LoggerFactory.getLogger(getClass)
        logger.info("\n\n\n  STARTING TEST " + testClass.getName + '.' +
            testName + "\n\n")
      }
    }
    vm0.invoke(startNode)
    vm1.invoke(startNode)
    vm2.invoke(startNode)

    // start lead node in this VM
    val sc = SnappyContext.globalSparkContext
    if (sc == null || sc.isStopped) {
      startSnappyLead(locatorPort, bootProps)
    }
    assert(ServiceManager.currentFabricServiceInstance.status ==
        FabricService.State.RUNNING)
    logger.info("\n\n\n  STARTING TEST " + testClass.getName + '.' +
        testName + "\n\n")
  }

  override def tearDown2(): Unit = {
    GemFireXDUtils.IS_TEST_MODE = false
    cleanupTestData(getClass.getName, getName)
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(getClass, "cleanupTestData",
      Array[AnyRef](getClass.getName, getName)))
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
    stopNetworkServers()
    bootProps.clear()
    val locNetPort = locatorNetPort
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        if (locNetPort > 0) {
          val loc = ServiceManager.getLocatorInstance
          if (loc != null) {
            loc.stopAllNetworkServers()
          }
        }
      }
    })
  }
}

/**
 * New utility methods would need to be added as and when corresponding
 * snappy code gets added.
 */
object ClusterManagerTestBase {
  val logger = LoggerFactory.getLogger(getClass)

  /* SparkContext is initialized on the lead node and hence,
  this can be used only by jobs running on Lead node */
  def sc: SparkContext = SnappyContext.globalSparkContext

  /**
   * Start a snappy lead. This code starts a Spark server and at the same time
   * also starts a SparkContext and hence it kind of becomes lead. We will use
   * LeadImpl once the code for that is ready.
   *
   * Only a single instance of SnappyLead should be started.
   */
  def startSnappyLead(locatorPort: Int, props: Properties): Unit = {
    // bootProps.setProperty("log-level", "fine")
    SparkContext.registerClusterManager(SnappyEmbeddedModeClusterManager)
    val conf: SparkConf = new SparkConf()
        .setMaster(s"snappydata://localhost[$locatorPort]")
        .setAppName("myapp")

    new File("./" + "driver").mkdir()
    new File("./" + "driver/events").mkdir()

    val dataDirForDriver = new File("./" + "driver/data").getAbsolutePath
    val eventDirForDriver = new File("./" + "driver/events").getAbsolutePath
    conf.set("spark.local.dir", dataDirForDriver)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", eventDirForDriver)
    conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "1000")

    props.asScala.foreach({ case (k, v) =>
      if (k.indexOf(".") < 0) {
        conf.set(io.snappydata.Constant.STORE_PROPERTY_PREFIX + k, v)
      }
      else {
        conf.set(k, v)
      }
    })
    logger.info(s"About to create SparkContext with conf \n" + conf.toDebugString)
    val sc = new SparkContext(conf)
    logger.info("SparkContext CREATED, about to create SnappyContext.")
    SnappyContext(sc)
    assert(ServiceManager.getServerInstance.status == FabricService.State.RUNNING)
    logger.info("SnappyContext CREATED successfully.")
    val lead: Server = ServiceManager.getServerInstance
    assert(lead.status == FabricService.State.RUNNING)
  }

  /**
   * Start a snappy server. Any number of snappy servers can be started.
   */
  def startSnappyServer(locatorPort: Int, props: Properties): Unit = {
    props.setProperty("locators", "localhost[" + locatorPort + ']')
    // bootProps.setProperty("log-level", "info")
    val server: Server = ServiceManager.getServerInstance
    server.start(props)
    assert(server.status == FabricService.State.RUNNING)
  }

  def startNetServer(netPort: Int): Unit = {
    ServiceManager.getServerInstance.startNetworkServer("localhost",
      netPort, null)
  }

  def cleanupTestData(testClass: String, testName: String): Unit = {
    // cleanup metastore
    val snc = SnappyContext()
    if (snc != null) {
      snc.catalog.getTables(None).foreach {
        case (tableName, false) =>
          snc.dropExternalTable(tableName, ifExists = true)
        case _ =>
      }
    }
    if (testName != null) {
      logger.info("\n\n\n  ENDING TEST " + testClass + '.' + testName + "\n\n")
    }
  }

  def stopSpark(): Unit = {
    // cleanup metastore
    cleanupTestData(null, null)
    SnappyContext.stop()
  }

  def stopNetworkServers(): Unit = {
    val service = ServiceManager.currentFabricServiceInstance
    if (service != null) {
      service.stopAllNetworkServers()
    }
  }

  def stopAny(): Unit = {
    val service = ServiceManager.currentFabricServiceInstance
    if (service != null) {
      service.stop(null)
    }
  }
}
