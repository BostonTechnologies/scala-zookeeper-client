package com.twitter.zookeeper

import org.apache.zookeeper.Watcher.Event.KeeperState

case class ZooKeeperClientConfiguration(servers: String, sessionTimeout: Int = 3000, connectTimeout: Int = 3000,
                      reconnectOnExpiration: Boolean = false, basePath: String = "",
                      sessionWatcher: Option[(ZooKeeperClient, KeeperState) => Unit] = None)

object ZooKeeperClientConfiguration {
  def apply(servers: String, sessionTimeout: Int,
            sessionWatcher: (ZooKeeperClient, KeeperState) => Unit): ZooKeeperClientConfiguration = {
    ZooKeeperClientConfiguration(servers, sessionTimeout, sessionWatcher = Option(sessionWatcher))
  }
}