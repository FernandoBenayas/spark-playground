// Using org.apache.spark.sql so we can accesss internal methods
package org.apache.spark.sql

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import java.sql.Timestamp

/*
UDAF - Remember, AGGREGATIONS: from a set of values, you get one value
Let's start by implementing a simple UDAF: instead of average (aka arithmetic mean), let's get the harmonic mean

Note: you will find lots of tutorials using UserDefinedAggregateFunction class instead of Aggregator. DON'T follow them. That class
has severe performance issues and has been deprecated (https://spark.apache.org/docs/3.1.2/api/scala/org/apache/spark/sql/expressions/UserDefinedAggregateFunction.html)
*/
case class Datapoint(var timestamp: java.sql.Timestamp, var value: Long, var index: Long)
case class HarmonicMeanParams(var numerator: Long, var denominator: Double)
case class ArithmeticMeanParams(var numerator: Long, var denominator: Double)

object HarmonicMean extends Aggregator[Datapoint, HarmonicMeanParams, Double] {

    def zero: HarmonicMeanParams = HarmonicMeanParams(0L, 0.0)

    def reduce(b: HarmonicMeanParams, a: Datapoint): HarmonicMeanParams = {
        b.numerator += 1L
        b.denominator += (1.0/(a.value)).toDouble
        b
    }

    def merge(b1: HarmonicMeanParams, b2: HarmonicMeanParams): HarmonicMeanParams = {
        b1.numerator += b2.numerator
        b1.denominator += b2.denominator
        b1
    }

    def finish(b: HarmonicMeanParams): Double = {
        b.numerator / b.denominator
    }

    def bufferEncoder: Encoder[HarmonicMeanParams] = Encoders.product
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}