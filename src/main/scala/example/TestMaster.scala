package example

import java.util.Collections
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.records.{ Container, ContainerLaunchContext, ContainerStatus, FinalApplicationStatus, Priority, Resource }
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import java.util.HashMap
import org.apache.hadoop.yarn.client.api.async.{ AMRMClientAsync, NMClientAsync }
import org.apache.hadoop.yarn.api.records.NodeReport
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records.{ ContainerId, NodeId }
import java.nio.ByteBuffer
import java.util.LinkedList
import java.util.concurrent.atomic.AtomicInteger

object TestMaster {

  // Initialize clients to ResourceManager and NodeManagers
  val conf = new YarnConfiguration();

  val rmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](100, TestAMNMCallback)
  rmClient.init(conf)
  rmClient.start()

  var loop = true
  var finalstatus = FinalApplicationStatus.SUCCEEDED

  val expectedContainer = new AtomicInteger();
  val mem = 1024
  val core = 1


  def main(args: Array[String]): Unit = {
    expectedContainer.set(args(0).toInt)
    val hosts = args(1).split(",")

    val relaxLocality = args(2).toBoolean

    rmClient.registerApplicationMaster("", 0, null)

    // Priority for worker containers - priorities are intra-application
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    // Resource requirements for worker containers
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(mem)
    capability.setVirtualCores(core)

    while (expectedContainer.get() > 0) {
      requestContainer(capability, hosts, priority, relaxLocality)
      // Make container requests to ResourceManager

      Thread.sleep(2000)
    }

    rmClient.unregisterApplicationMaster(finalstatus, "", "")

  }

  def requestContainer(capability: Resource, hosts: Array[String], priority: Priority, relaxLocality: Boolean) {
      (1 to expectedContainer.get()).foreach(i => {
        println("Try to get containers from hosts " + hosts.mkString(",") + ", relaxLocality: " + relaxLocality)
        val containerAsk = new ContainerRequest(capability, hosts, null, priority, relaxLocality)
        rmClient.addContainerRequest(containerAsk)
      })
  }


}

object TestAMNMCallback extends AMRMClientAsync.CallbackHandler with NMClientAsync.CallbackHandler {

  val runningContainer = scala.collection.mutable.Map[ContainerId, NodeId]()

  val nmClient = NMClientAsync.createNMClientAsync(this)
  nmClient.init(TestMaster.conf)
  nmClient.start()

  // Handle async RM callback
  def onContainersAllocated(containers: java.util.List[Container]) {

    for (c <- containers) {
      println("Allocated container " + c)
      if (checkContainer(c)) {
        // if the container is valid run the kafka broker command
        runCmd(c)
      } else {
        // release this assigned container
        TestMaster.rmClient.releaseAssignedContainer(c.getId());
      }
    }

    def runCmd(c: Container) {
      try {
          println("start container")
        val ctx =
          Records.newRecord(classOf[ContainerLaunchContext])
        ctx.setCommands(Collections.singletonList("sleep 1m"))
        TestMaster.expectedContainer.getAndDecrement();
        println("[AM] Launching container " + c.getId())
        nmClient.startContainerAsync(c, ctx)
        runningContainer += (c.getId() -> c.getNodeId())

      } catch {
        case ex: Exception => println("[AM] Error launching container " + c.getId() + " " + ex)
      }
    }

    def checkContainer(c: Container) = {
      c.getResource().getMemory() == TestMaster.mem &&
        c.getResource().getVirtualCores() == TestMaster.core &&
        !runningContainer.values.exists(_ == c.getNodeId())
    }

  }

  def onContainersCompleted(statuses: java.util.List[ContainerStatus]) {
    for (status <- statuses) {
      println("[AM] Completed container " + status.getContainerId() + " status:" + status)
    }
  }

  def onNodesUpdated(updated: java.util.List[NodeReport]) {
  }

  def onReboot() {
  }

  def onShutdownRequest() {
    TestMaster.finalstatus = FinalApplicationStatus.KILLED
    runningContainer.foreach((cinfo) => {
      nmClient.stopContainerAsync(cinfo._1, cinfo._2)
    })
    TestMaster.loop = false
  }

  def onError(t: Throwable) {
    TestMaster.finalstatus = FinalApplicationStatus.FAILED
    runningContainer.foreach((cinfo) => {
      nmClient.stopContainerAsync(cinfo._1, cinfo._2)
    })
    TestMaster.loop = false
  }

  def getProgress(): Float = {
    0
  }

  //handle async NM callback
  def onContainerStopped(containerId: ContainerId) {
    println("Succeeded to stop Container " + containerId);
    runningContainer - containerId
  }

  def onContainerStatusReceived(containerId: ContainerId,
    containerStatus: ContainerStatus) {
    println("Container Status: id=" + containerId + ", status=" +
      containerStatus);
  }

  def onContainerStarted(containerId: ContainerId,
    allServiceResponse: java.util.Map[String, ByteBuffer]) {
    println("Succeeded to start Container " + containerId);

  }

  def onStartContainerError(containerId: ContainerId, t: Throwable) {
    sys.error("Error running container: " + containerId + "\n" + t)
    runningContainer - containerId
  }

  def onGetContainerStatusError(
    containerId: ContainerId, t: Throwable) {
    sys.error("Failed to query the status of Container " + containerId);
  }

  def onStopContainerError(containerId: ContainerId, t: Throwable) {
    sys.error("Failed to stop Container " + containerId);
  }

}


