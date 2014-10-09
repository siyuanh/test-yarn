package example

import java.io.File
import java.io.IOException
import java.util.Collections
import java.util.HashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path }
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{
  ApplicationId,
  ApplicationReport,
  ApplicationSubmissionContext,
  ContainerLaunchContext,
  LocalResource,
  LocalResourceType,
  LocalResourceVisibility,
  Resource,
  YarnApplicationState
}
import org.apache.hadoop.yarn.client.api.{ YarnClient, YarnClientApplication }
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ Apps, ConverterUtils, Records }
import scala.collection.JavaConversions._
import java.util.zip.ZipOutputStream
import java.io.FileOutputStream
import java.io.FileInputStream
import java.util.zip.ZipEntry
import scala.collection.Parallel

object TestClient {

  val conf = new YarnConfiguration

  val fs = FileSystem.get(conf)

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Missing parameters \n \t Usage: hadoop -jar ... example.TestClient hdfsResource #containers hosts(separate by ',') relaxation")
      println("\t Ex: hadoop -jar ... example.TestClient /user/hadoop/test 3 node1,node2 false")
      System.exit(1)
    }

    val hdfsRes = new Path(args(0))
    val amMem = 512
    val amCore = 1
    val queue = "default"

    val yarnClient = YarnClient.createYarnClient
    yarnClient.init(conf)
    yarnClient.start()
    println(conf.get(YarnConfiguration.RM_ADDRESS))

    // Create application via yarnClient
    val app = yarnClient.createApplication()

    // Finally, set-up ApplicationSubmissionContext for the application
    val appContext =
      app.getApplicationSubmissionContext()
    // Submit application
    val appId = appContext.getApplicationId()
    println("Submitting application " + appId)

    // Set up the container launch context for the application master
    val amContainer =
      Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setCommands(
      Collections.singletonList(
        "pwd > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/test.log;" + "$JAVA_HOME/bin/java" +
          " -Xmx" + amMem + "M" +
          " example.TestMaster" +
          " " + args(1) + " " + args(2) + " " + args(3) +
          " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.out" +
          " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.err"))

    // Setup CLASSPATH for ApplicationMaster
    // copy file to hdfs
    val jarPath = TestMaster.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
    val scalaJar = language.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()

    println("copy jars to hdfs")
    fs.copyFromLocalFile(false, true, new Path(jarPath), hdfsRes)
    fs.copyFromLocalFile(false, true, new Path(scalaJar), hdfsRes)

    val jarMap = getLocalResource(hdfsRes)

    println("setup local resource")
    amContainer.setLocalResources(jarMap)
    
    amContainer.setEnvironment(getAppEnv)

    // Set up resource type requirements for ApplicationMaster
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(amMem)
    capability.setVirtualCores(amCore)

    appContext.setApplicationName("Test-on-Yarn") // application name
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(capability)
    appContext.setQueue(queue) // queue 

    yarnClient.submitApplication(appContext)

    var appReport = yarnClient.getApplicationReport(appId);
    var appState = appReport.getYarnApplicationState();
    while (appState != YarnApplicationState.FINISHED &&
      appState != YarnApplicationState.KILLED &&
      appState != YarnApplicationState.FAILED) {
      Thread.sleep(100)
      appReport = yarnClient.getApplicationReport(appId)
      appState = appReport.getYarnApplicationState()
    }

    println(
      "Application " + appId + " finished with" +
        " state " + appState +
        " at " + appReport.getFinishTime())

  }

  def getLocalResource(jarFolder: Path): scala.collection.mutable.Map[String, LocalResource] = {
    val returnVal = scala.collection.mutable.Map[String, LocalResource]()
    for(fst <- fs.listStatus(jarFolder)){
      val localResouceFile = Records.newRecord(classOf[LocalResource])
      localResouceFile.setResource(ConverterUtils.getYarnUrlFromPath(fst.getPath()))
      localResouceFile.setType(LocalResourceType.FILE)
      localResouceFile.setSize(fst.getLen())
      localResouceFile.setTimestamp(fst.getModificationTime())
      localResouceFile.setVisibility(LocalResourceVisibility.APPLICATION)
      returnVal += (fst.getPath().getName() -> localResouceFile)
    }
    returnVal
  }

  def getAppEnv(): Map[String, String] = {
    val appMasterEnv = new HashMap[String, String]()
    for (
      c <- conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(","))
    ) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
        c.trim());
    }
    Apps.addToEnvironment(appMasterEnv,
      Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*")
    appMasterEnv.toMap
  }

}
