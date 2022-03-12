// Using org.apache.spark.sql so we can accesss internal methods
package org.apache.spark.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/*
UDF - Remember, TRANSFORMATIONS: one-on-one function: from a set of n values, you get another set of n values
Let's start by implementing a simple UDF: 
*/