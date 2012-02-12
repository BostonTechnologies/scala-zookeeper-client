package com.twitter.zookeeper

import java.net.{Socket, ConnectException}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper, CreateMode}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.slf4j.LoggerFactory
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.apache.zookeeper.CreateMode._
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.data.Stat
import org.specs._
import scala.collection.mutable

class ZooKeeperClientSpec extends Specification {
  val Logger = LoggerFactory.getLogger(classOf[ZooKeeperClientSpec])

  val serverAddress = "localhost:2181"

  doBeforeSpec {
    // we need to be sure that a ZooKeeper server is running in order to test
    Logger.info("Testing connection to ZooKeeper server at %s...".format(serverAddress))
    val socketPort = serverAddress.split(":")
    new Socket(socketPort(0), socketPort(1).toInt) mustNot throwA[ConnectException]
  }

  "ZookeeperClient" should {
    shareVariables()
    var zkClient : ZooKeeperClient = null

    doFirst {
      Logger.info("Attempting to connect to ZooKeeper server %s...".format(serverAddress))
      zkClient = new ZooKeeperClient(serverAddress)
    }

    doLast {
      zkClient.close()
    }

    "be able to be instantiated with a FakeWatcher" in {
      zkClient mustNot beNull
    }

    "connect to local Zookeeper server and retrieve version" in {
      zkClient.isAlive mustBe true
    }

    "get data at a known-good specified path" in {
      val results: Array[Byte] = zkClient.get("/")
      results.size must beGreaterThanOrEqualTo(0)
    }

    "get data at a known-bad specified path" in {
      zkClient.get("/thisdoesnotexist") must throwA[NoNodeException]
    }

    "get list of children" in {
      zkClient.getChildren("/") must notBeEmpty
    }

    "create a node at a specified path" in {
      val data = Array[Byte](0x63)
      val createMode = EPHEMERAL

      zkClient.create("/foo", data, createMode) mustEqual "/foo"
      zkClient.delete("/foo")
    }

    "watch a node" in {
      val data = Array[Byte](0x63)
      val node = "/datanode"
      val createMode = EPHEMERAL
      var watchCount = 0
      def watcher(data : Option[Array[Byte]], stat : Stat) {
        watchCount += 1
      }
      zkClient.create(node, data, createMode)
      zkClient.watchNode(node, watcher)
      Thread.sleep(50L)
      watchCount mustEqual 1
      zkClient.delete("/datanode")
    }

    "watch a tree of nodes" in {
      var children : Seq[String] = List()
      var watchCount = 0
      def watcher(nodes : Seq[String]) {
        watchCount += 1
        children = nodes
      }
      zkClient.createPath("/tree/a")
      zkClient.createPath("/tree/b")
      zkClient.watchChildren("/tree", watcher)
      children.size mustEqual 2
      children must containAll(List("a", "b"))
      watchCount mustEqual 1
      zkClient.createPath("/tree/c")
      Thread.sleep(50L)
      children.size mustEqual 3
      children must containAll(List("a", "b", "c"))
      watchCount mustEqual 2
      zkClient.delete("/tree/a")
      Thread.sleep(50L)
      children.size mustEqual 2
      children must containAll(List("b", "c"))
      watchCount mustEqual 3
      zkClient.deleteRecursive("/tree")
    }

    "watch a tree of nodes with data" in {
      def mkNode(node : String) {
        zkClient.create("/root/" + node, node.getBytes, CreateMode.EPHEMERAL)
      }
      val children : mutable.Map[String,String] = mutable.Map()
      var watchCount = 0
      def notifier(child : String) {
        watchCount += 1
        if (children.contains(child)) {
          children(child) mustEqual child
        }
      }
      zkClient.createPath("/root")
      mkNode("a")
      mkNode("b")
      zkClient.watchChildrenWithData("/root", children,
                                     {(b : Array[Byte]) => new String(b)}, notifier)
      children.size mustEqual 2
      children.keySet must containAll(List("a", "b"))
      watchCount mustEqual 2
      mkNode("c")
      Thread.sleep(50L)
      children.size mustEqual 3
      children.keySet must containAll(List("a", "b", "c"))
      watchCount mustEqual 3
      zkClient.delete("/root/a")
      Thread.sleep(50L)
      children.size mustEqual 2
      children.keySet must containAll(List("b", "c"))
      watchCount mustEqual 4
      zkClient.deleteRecursive("/root")
    }

    "call default watcher in the event of a session expiration" in {
      val sessionExpiredLatch = new CountDownLatch(1)
      val sessionWatcher = (zk: ZooKeeperClient, state: KeeperState) => {
        Logger.debug("Session watcher called with state: "+state)
        if(state == KeeperState.Expired) {
          sessionExpiredLatch.countDown()
        }
      }
      val sessionWatchingClient = new ZooKeeperClient("localhost", 1500, sessionWatcher = Some(sessionWatcher))

      val testNodePath = "/helpme!"

      val node = sessionWatchingClient.create(testNodePath, Array.empty[Byte], CreateMode.EPHEMERAL)
      node must notBeNull

      //Make session expire by connecting with other client's session credentials,
      // see Apache ZooKeeper docs for more info on this strategy
      val connectLatch = new CountDownLatch(1)
      val y = new ZooKeeper(serverAddress, 3000, new Watcher {
        def process(event: WatchedEvent) {
          if(event.getState == KeeperState.SyncConnected) {
            connectLatch.countDown()
          }
          Logger.debug("Default Watcher event second zk client: " + event)
        }
      }, sessionWatchingClient.getHandle.getSessionId, sessionWatchingClient.getHandle.getSessionPasswd)
      connectLatch.await(2000, TimeUnit.MILLISECONDS) must_== true

      //close the second client
      y.close()
      
      //Wait for session expired event to be triggered on original client
      sessionExpiredLatch.await(4000l, TimeUnit.MILLISECONDS) must_== true
    }

  }
}
