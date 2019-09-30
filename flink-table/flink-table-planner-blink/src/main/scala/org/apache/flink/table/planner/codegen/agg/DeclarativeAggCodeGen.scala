/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.codegen.agg

import org.apache.flink.table.expressions._
<<<<<<< HEAD
import org.apache.flink.table.expressions.utils.ApiExpressionUtils
import org.apache.flink.table.planner.codegen.CodeGenUtils.primitiveTypeTermForType
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator.DISTINCT_KEY_TERM
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.expressions.{ResolvedAggInputReference, ResolvedAggLocalReference, ResolvedDistinctKeyReference, RexNodeConverter}
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.logical.LogicalType
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.functions.BuiltInFunctionDefinitions

import scala.collection.JavaConverters._
=======
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator.DISTINCT_KEY_TERM
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver.{toRexDistinctKey, toRexInputRef}
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.expressions.{DeclarativeExpressionResolver, RexNodeExpression}
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.{fromDataTypeToLogicalType, fromLogicalTypeToDataType}
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder
>>>>>>> release-1.9

/**
  * It is for code generate aggregation functions that are specified using expressions.
  * The aggregate buffer is embedded inside of a larger shared aggregation buffer.
  *
  * @param ctx the code gen context
  * @param aggInfo  the aggregate information
  * @param filterExpression filter argument access expression, none if no filter
  * @param mergedAccOffset the mergedAcc may come from local aggregate,
  *                        this is the first buffer offset in the row
  * @param aggBufferOffset  the offset in the buffers of this aggregate
  * @param aggBufferSize  the total size of aggregate buffers
  * @param inputTypes   the input field type infos
<<<<<<< HEAD
  * @param constantExprs  the constant expressions
=======
  * @param constants  the constant literals
>>>>>>> release-1.9
  * @param relBuilder  the rel builder to translate expressions to calcite rex nodes
  */
class DeclarativeAggCodeGen(
    ctx: CodeGeneratorContext,
    aggInfo: AggregateInfo,
    filterExpression: Option[Expression],
    mergedAccOffset: Int,
    aggBufferOffset: Int,
    aggBufferSize: Int,
    inputTypes: Seq[LogicalType],
<<<<<<< HEAD
    constantExprs: Seq[GeneratedExpression],
=======
    constants: Seq[RexLiteral],
>>>>>>> release-1.9
    relBuilder: RelBuilder)
  extends AggCodeGen {

  private val function = aggInfo.function.asInstanceOf[DeclarativeAggregateFunction]

  private val bufferTypes = aggInfo.externalAccTypes.map(fromDataTypeToLogicalType)
  private val bufferIndexes = Array.range(aggBufferOffset, aggBufferOffset + bufferTypes.length)
  private val bufferTerms = function.aggBufferAttributes
      .map(a => s"agg${aggInfo.aggIndex}_${a.getName}")
<<<<<<< HEAD
  private val bufferNullTerms = bufferTerms.map(_ + "_isNull")

  private val argIndexes = aggInfo.argIndexes
  private val argTypes = {
    val types = inputTypes ++ constantExprs.map(_.resultType)
    argIndexes.map(types(_))
  }
  private val rexNodeGen = new RexNodeConverter(relBuilder)
=======

  private val rexNodeGen = new ExpressionConverter(relBuilder)

  private val bufferNullTerms = {
    val exprCodegen = new ExprCodeGenerator(ctx, false)
    bufferTerms.zip(bufferTypes).map {
      case (name, t) => new LocalReferenceExpression(name, fromLogicalTypeToDataType(t))
    }.map(_.accept(rexNodeGen)).map(exprCodegen.generateExpression).map(_.nullTerm)
  }

  private val argIndexes = aggInfo.argIndexes
  private val argTypes = {
    val types = inputTypes ++ constants.map(t => FlinkTypeFactory.toLogicalType(t.getType))
    argIndexes.map(types(_))
  }
>>>>>>> release-1.9

  def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    function.initialValuesExpressions
      .map(expr => generator.generateExpression(expr.accept(rexNodeGen)))
  }

  def setAccumulator(generator: ExprCodeGenerator): String = {
    val aggBufferAccesses = function.aggBufferAttributes.zipWithIndex
      .map { case (attr, index) =>
<<<<<<< HEAD
        new ResolvedAggInputReference(
          attr.getName, bufferIndexes(index), bufferTypes(index))
=======
        toRexInputRef(relBuilder, bufferIndexes(index), bufferTypes(index))
>>>>>>> release-1.9
      }
      .map(expr => generator.generateExpression(expr.accept(rexNodeGen)))

    val setters = aggBufferAccesses.zipWithIndex.map {
      case (access, index) =>
<<<<<<< HEAD
        val typeTerm = primitiveTypeTermForType(access.resultType)
        val memberName = bufferTerms(index)
        val memberNullTerm = bufferNullTerms(index)
        ctx.addReusableMember(s"private $typeTerm $memberName;")
        ctx.addReusableMember(s"private boolean $memberNullTerm;")
        s"""
           |${access.copyResultTermToTargetIfChanged(ctx, memberName)};
           |$memberNullTerm = ${access.nullTerm};
=======
        s"""
           |${access.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
           |${bufferNullTerms(index)} = ${access.nullTerm};
>>>>>>> release-1.9
         """.stripMargin
    }

    setters.mkString("\n")
  }

  override def resetAccumulator(generator: ExprCodeGenerator): String = {
    val initialExprs = function.initialValuesExpressions
      .map(expr => generator.generateExpression(expr.accept(rexNodeGen)))
    val codes = initialExprs.zipWithIndex.map {
      case (init, index) =>
        val memberName = bufferTerms(index)
        val memberNullTerm = bufferNullTerms(index)
        s"""
           |${init.code}
           |$memberName = ${init.resultTerm};
           |$memberNullTerm = ${init.nullTerm};
         """.stripMargin
    }
    codes.mkString("\n")
  }

  def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    bufferTypes.zipWithIndex.map { case (bufferType, index) =>
      GeneratedExpression(
        bufferTerms(index), bufferNullTerms(index), "", bufferType)
    }
  }

  def accumulate(generator: ExprCodeGenerator): String = {
    val isDistinctMerge = generator.input1Term.startsWith(DISTINCT_KEY_TERM)
    val resolvedExprs = function.accumulateExpressions
      .map(_.accept(ResolveReference(isDistinctMerge = isDistinctMerge)))

    val exprs = resolvedExprs
      .map(_.accept(rexNodeGen)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map { case (expr, index) =>
      s"""
         |${expr.code}
         |${expr.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
         |${bufferNullTerms(index)} = ${expr.nullTerm};
       """.stripMargin
    }

    filterExpression match {
      case Some(expr) =>
        val generated = generator.generateExpression(expr.accept(rexNodeGen))
        s"""
           |if (${generated.resultTerm}) {
           |  ${codes.mkString("\n")}
           |}
         """.stripMargin
      case None =>
        codes.mkString("\n")
    }
  }

  def retract(generator: ExprCodeGenerator): String = {
    val isDistinctMerge = generator.input1Term.startsWith(DISTINCT_KEY_TERM)
    val resolvedExprs = function.retractExpressions
      .map(_.accept(ResolveReference(isDistinctMerge = isDistinctMerge)))

    val exprs = resolvedExprs
      .map(_.accept(rexNodeGen)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map { case (expr, index) =>
      s"""
         |${expr.code}
         |${expr.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
         |${bufferNullTerms(index)} = ${expr.nullTerm};
       """.stripMargin
    }

    filterExpression match {
      case Some(expr) =>
        val generated = generator.generateExpression(expr.accept(rexNodeGen))
        s"""
           |if (${generated.resultTerm}) {
           |  ${codes.mkString("\n")}
           |}
         """.stripMargin
      case None =>
        codes.mkString("\n")
    }
  }

  def merge(generator: ExprCodeGenerator): String = {
    val exprs = function.mergeExpressions
      .map(_.accept(ResolveReference(isMerge = true)))
      .map(_.accept(rexNodeGen)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map { case (expr, index) =>
      s"""
         |${expr.code}
         |${expr.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
         |${bufferNullTerms(index)} = ${expr.nullTerm};
       """.stripMargin
    }

    codes.mkString("\n")
  }

  def getValue(generator: ExprCodeGenerator): GeneratedExpression = {
    val resolvedGetValueExpression = function.getValueExpression
      .accept(ResolveReference())
    generator.generateExpression(resolvedGetValueExpression.accept(rexNodeGen))
  }

  /**
    * Resolves the given expression to a resolved Expression.
    *
    * @param isMerge this is called from merge() method
    */
  private case class ResolveReference(
      isMerge: Boolean = false,
<<<<<<< HEAD
      isDistinctMerge: Boolean = false) extends ExpressionVisitor[Expression] {

    override def visit(call: CallExpression): Expression = ???

    override def visit(valueLiteralExpression: ValueLiteralExpression): Expression = {
      valueLiteralExpression
    }

    override def visit(input: FieldReferenceExpression): Expression = {
      input
    }

    override def visit(typeLiteral: TypeLiteralExpression): Expression = {
      typeLiteral
    }

    private def visitUnresolvedCallExpression(
        unresolvedCall: UnresolvedCallExpression): Expression = {
      ApiExpressionUtils.unresolvedCall(
        unresolvedCall.getFunctionDefinition,
        unresolvedCall.getChildren.asScala.map(_.accept(this)): _*)
    }

    private def visitUnresolvedReference(input: UnresolvedReferenceExpression)
      : Expression = {
      function.aggBufferAttributes.indexOf(input) match {
        case -1 =>
          // Not find in agg buffers, it is a operand, represent reference of input field.
          // In non-merge case, the input is the operand of the aggregate function.
          // In merge case, the input is the aggregate buffers sent by local aggregate.
          if (isMerge) {
            val localIndex = function.mergeOperands.indexOf(input)
            // in merge case, the input1 is mergedAcc
            new ResolvedAggInputReference(
              input.getName,
              mergedAccOffset + bufferIndexes(localIndex),
              bufferTypes(localIndex))
          } else {
            val localIndex = function.operands.indexOf(input)
            val inputIndex = argIndexes(localIndex)
            if (inputIndex >= inputTypes.length) { // it is a constant
              val constantIndex = inputIndex - inputTypes.length
              val constantTerm = constantExprs(constantIndex).resultTerm
              val nullTerm = constantExprs(constantIndex).nullTerm
              val constantType = constantExprs(constantIndex).resultType
              // constant is reused as member variable
              new ResolvedAggLocalReference(
                constantTerm,
                nullTerm,
                constantType)
            } else { // it is a input field
              if (isDistinctMerge) {  // this is called from distinct merge
                if (function.operandCount == 1) {
                  // the distinct key is a BoxedValue
                  new ResolvedDistinctKeyReference(input.getName, argTypes(localIndex))
                } else {
                  // the distinct key is a BaseRow
                  new ResolvedAggInputReference(input.getName, localIndex, argTypes(localIndex))
                }
              } else {
                // the input is the inputRow
                new ResolvedAggInputReference(
                  input.getName, argIndexes(localIndex), argTypes(localIndex))
              }
            }
          }
        case localIndex =>
          // it is a agg buffer.
          val name = bufferTerms(localIndex)
          val nullTerm = bufferNullTerms(localIndex)
          // buffer access is reused as member variable
          new ResolvedAggLocalReference(name, nullTerm, bufferTypes(localIndex))
      }
    }

    override def visit(other: Expression): Expression = {
      other match {
        case u : UnresolvedReferenceExpression => visitUnresolvedReference(u)
        case u : UnresolvedCallExpression => visitUnresolvedCallExpression(u)
        case _ => other
      }
=======
      isDistinctMerge: Boolean = false)
    extends DeclarativeExpressionResolver(relBuilder, function, isMerge) {

    override def toMergeInputExpr(name: String, localIndex: Int): ResolvedExpression = {
      // in merge case, the input1 is mergedAcc
      toRexInputRef(
        relBuilder,
        mergedAccOffset + bufferIndexes(localIndex),
        bufferTypes(localIndex))
    }

    override def toAccInputExpr(name: String, localIndex: Int): ResolvedExpression = {
      val inputIndex = argIndexes(localIndex)
      if (inputIndex >= inputTypes.length) { // it is a constant
        val constantIndex = inputIndex - inputTypes.length
        val constant = constants(constantIndex)
        new RexNodeExpression(constant,
          fromLogicalTypeToDataType(FlinkTypeFactory.toLogicalType(constant.getType)))
      } else { // it is a input field
        if (isDistinctMerge) { // this is called from distinct merge
          if (function.operandCount == 1) {
            // the distinct key is a BoxedValue
            val t = argTypes(localIndex)
            toRexDistinctKey(relBuilder, name, t)
          } else {
            // the distinct key is a BaseRow
            toRexInputRef(relBuilder, localIndex, argTypes(localIndex))
          }
        } else {
          // the input is the inputRow
          toRexInputRef(relBuilder, argIndexes(localIndex), argTypes(localIndex))
        }
      }
    }

    override def toAggBufferExpr(name: String, localIndex: Int): ResolvedExpression = {
      // name => agg${aggInfo.aggIndex}_$name"
      new LocalReferenceExpression(
        bufferTerms(localIndex),
        fromLogicalTypeToDataType(bufferTypes(localIndex)))
>>>>>>> release-1.9
    }
  }

  override def checkNeededMethods(
     needAccumulate: Boolean = false,
     needRetract: Boolean = false,
     needMerge: Boolean = false,
<<<<<<< HEAD
     needReset: Boolean = false): Unit = {
=======
     needReset: Boolean = false,
     needEmitValue: Boolean = false): Unit = {
>>>>>>> release-1.9
    // skip the check for DeclarativeAggregateFunction for now
  }
}
