package com.github.rodrigopr.recsys.utils

import com.redis.RedisClientPool

object RedisUtil {
  val pool = new RedisClientPool("localhost", 6379, 200)
  def buildKey(keys: String*) = keys.mkString(":")
  def buildKey(keys: List[String]) = keys.mkString(":")
}
