package com.github.rodrigopr.recsys.utils

import scala.collection.JavaConversions._
import collection.mutable
import java.util.concurrent.ConcurrentHashMap

/**
 * A memoized unary function.
 *
 * @param f A unary function to memoize
 * @param [T] the argument type
 * @param [R] the return type
 */
class Memoize1[-T, +R](f: T => R) extends (T => R) {
  import scala.collection.mutable
  // map that stores (argument, result) pairs
  private[this] val vals: mutable.ConcurrentMap[T, R] = new ConcurrentHashMap[T, R]

  // register the memoized data for future cleanup
  Memoize.allResults.add(0, vals)

  // Given an argument x,
  //   If vals contains x return vals(x).
  //   Otherwise, update vals so that vals(x) == f(x) and return f(x).
  def apply(x: T): R = vals getOrElseUpdate (x, f(x))
}

object Memoize {
  def clean() {
    allResults.foreach(_.clear())
  }

  /**
   * hold all results map being memoized, util for memory cleanup
   */
  val allResults =  new mutable.LinkedList[mutable.ConcurrentMap[_,_]]

  /**
   * Memoize a unary (single-argument) function.
   *
   * @param f the unary function to memoize
   */
  def memoize[T, R](f: T => R): (T => R) = new Memoize1(f)

  /**
   * Memoize a binary (two-argument) function.
   *
   * @param f the binary function to memoize
   *
   * This works by turning a function that takes two arguments of type
   * T1 and T2 into a function that takes a single argument of type
   * (T1, T2), memoizing that "tupled" function, then "untupling" the
   * memoized function.
   */
  def memoize[T1, T2, R](f: (T1, T2) => R): ((T1, T2) => R) =
    Function.untupled(memoize(f.tupled))

  /**
   * Memoize a ternary (three-argument) function.
   *
   * @param f the ternary function to memoize
   */
  def memoize[T1, T2, T3, R](f: (T1, T2, T3) => R): ((T1, T2, T3) => R) =
    Function.untupled(memoize(f.tupled))

  // ... more memoize methods for higher-arity functions ...

  /**
   * Fixed-point combinator (for memoizing recursive functions).
   */
  def Y[T, R](f: (T => R) => T => R): (T => R) = {
    lazy val yf: (T => R) = memoize(f(yf)(_))
    yf
  }
}