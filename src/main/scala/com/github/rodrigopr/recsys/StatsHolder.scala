package com.github.rodrigopr.recsys

import java.util.concurrent.atomic.AtomicLong
import collection.mutable
import collection.mutable.ArrayBuffer

import com.github.rodrigopr.recsys.utils.ListAvg._

object StatsHolder {
  val counters = mutable.Map[String, AtomicLong]()
  val timers = mutable.Map[String, ArrayBuffer[Long]]()
  val data = mutable.Map[String, String]()

  def clear() {
    counters.synchronized(counters.clear())
    timers.synchronized(timers.clear())
  }

  def incr(counterName: String, amount: Long = 1) {
    counters.synchronized {
      if(!counters.contains(counterName)) {
        val counter = new AtomicLong(0l)
        counters.put(counterName, counter)
      }

      counters(counterName).addAndGet(amount)
    }
  }

  def setCustomData(name: String, data: String) = {
    this.data.put(name, data)
  }

  def timeIt[T](timerName: String, increment: Boolean = false, print: Boolean = false)(func: => T): T = {
    if (increment) {
      incr(timerName)
    }

    val t0 = System.currentTimeMillis
    val res = func
    val t1 = System.currentTimeMillis

    timers.synchronized {
      if(!timers.contains(timerName)) {
        val list = ArrayBuffer[Long]()
        timers.put(timerName, list)
      }

      timers(timerName) += t1 - t0

      if(print) {
        Console.println("Processed: " + timerName + " in " + (t1 - t0) + "ms - Total processed: " + timers(timerName).size)
      }
    }

    res
  }

  def printAll(print: String => Unit) {
    val allData = counters.map{case(n, v) => ("3 " + n) -> "Total '%s': %d\n".format(n, v.get)}
    allData ++= timers.map{case(n, v) => ("2 " + n) -> "Mean '%s': %.2fms\n".format(n, v.avg)}
    allData ++= data.map { case(n, v) => ("1" + n) -> "[C] %s: %s\n".format(n,v) }

    print("-------------========================-------------\n")
    print("Stats data: \n")
    allData.toList.sortBy(_._1).map(_._2).foreach(print)
  }
}
