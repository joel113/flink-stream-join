package com.joel.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object VehicleJoin {

  def main(args: Array[String]) {

    val streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val vehicleStream = streamExecutionEnvironment
      .socketTextStream("localhost", 9000)
      .map(vehicleTextStream => vehicleTextStream.split(" "))
      .filter(vehicleFields => vehicleFields.length == 3)
      .map(vehicleFields => Tuple3(Integer.parseInt(vehicleFields(0)), vehicleFields(1), vehicleFields(2)))

    val vehicleStateStream = streamExecutionEnvironment
      .socketTextStream("localhost", 9001)
      .map(stateTextStream => stateTextStream.split(" "))
      .filter(stateFields => stateFields.length == 3)
      .map(stateFields => Tuple3(Integer.parseInt(stateFields(0)), stateFields(1), stateFields(2)))

    val joinedVehicleStream = vehicleStream.join(vehicleStateStream)
      .where(vehicleJoinKey => vehicleJoinKey._1)
      .equalTo(vehicleStateJoinKey => vehicleStateJoinKey._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
      .apply {
        (vehicleDetail, vehicleStateDetail) =>
          Tuple4(vehicleDetail._2, vehicleDetail._3, vehicleStateDetail._2, vehicleStateDetail._3)
      }

    vehicleStream.print()
    vehicleStateStream.print()
    joinedVehicleStream.print

    streamExecutionEnvironment.execute("Vehicle Join Example")

  }

}
