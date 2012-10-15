package com.github.rodrigopr.recsys.scripts

import com.redis.RedisClientPool

class BaseGraphScript extends App {
  val pool = new RedisClientPool("localhost", 6379, 150)

  def buildKey(keys: String*) = keys.mkString(":")
  def buildKey(keys: List[String]) = keys.mkString(":")
}
