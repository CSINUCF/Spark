package org.ucf.spark

import testcase._
object SparkMain extends PreProcessData
object SparkMain_NA extends PreProcessData_NA

object SparkMain_FM extends FilterMove
object SparkMain_FM_O extends FilterMoveOptimzied

object SparkMain_MM extends MemoryManage
object SparkMain_MM_O extends MemoryManageOptimized

object SparkMain_OM extends OperationMigration
object SparkMain_OM_O extends OperationMigrationOptimzied

object SparkMain_UD extends UnusedData
object SparkMain_UD_O extends UnusedDataOptimized
