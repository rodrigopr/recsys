package com.github.rodrigopr.recsys.utils

object ListAvg {
  implicit def iterableWithAvg[T:Numeric](data:Iterable[T]) = new {
    def avg = average(data)

    def average( ts: Iterable[T] )(implicit num: Numeric[T] ) = {
      num.toDouble( ts.sum ) / ts.size
    }
  }
}
