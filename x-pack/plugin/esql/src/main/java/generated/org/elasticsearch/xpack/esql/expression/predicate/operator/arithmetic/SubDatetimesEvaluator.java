// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import java.lang.ArithmeticException;
import java.lang.Override;
import java.lang.String;
import java.time.DateTimeException;
import java.time.temporal.TemporalAmount;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Sub}.
 * This class is generated. Do not edit it.
 */
public final class SubDatetimesEvaluator implements EvalOperator.ExpressionEvaluator {
  private final Warnings warnings;

  private final EvalOperator.ExpressionEvaluator datetime;

  private final TemporalAmount temporalAmount;

  private final DriverContext driverContext;

  public SubDatetimesEvaluator(Source source, EvalOperator.ExpressionEvaluator datetime,
      TemporalAmount temporalAmount, DriverContext driverContext) {
    this.warnings = new Warnings(source);
    this.datetime = datetime;
    this.temporalAmount = temporalAmount;
    this.driverContext = driverContext;
  }

  @Override
  public Block.Ref eval(Page page) {
    try (Block.Ref datetimeRef = datetime.eval(page)) {
      if (datetimeRef.block().areAllValuesNull()) {
        return Block.Ref.floating(Block.constantNullBlock(page.getPositionCount()));
      }
      LongBlock datetimeBlock = (LongBlock) datetimeRef.block();
      LongVector datetimeVector = datetimeBlock.asVector();
      if (datetimeVector == null) {
        return Block.Ref.floating(eval(page.getPositionCount(), datetimeBlock));
      }
      return Block.Ref.floating(eval(page.getPositionCount(), datetimeVector));
    }
  }

  public LongBlock eval(int positionCount, LongBlock datetimeBlock) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      if (datetimeBlock.isNull(p) || datetimeBlock.getValueCount(p) != 1) {
        result.appendNull();
        continue position;
      }
      try {
        result.appendLong(Sub.processDatetimes(datetimeBlock.getLong(datetimeBlock.getFirstValueIndex(p)), temporalAmount));
      } catch (ArithmeticException | DateTimeException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  public LongBlock eval(int positionCount, LongVector datetimeVector) {
    LongBlock.Builder result = LongBlock.newBlockBuilder(positionCount);
    position: for (int p = 0; p < positionCount; p++) {
      try {
        result.appendLong(Sub.processDatetimes(datetimeVector.getLong(p), temporalAmount));
      } catch (ArithmeticException | DateTimeException e) {
        warnings.registerException(e);
        result.appendNull();
      }
    }
    return result.build();
  }

  @Override
  public String toString() {
    return "SubDatetimesEvaluator[" + "datetime=" + datetime + ", temporalAmount=" + temporalAmount + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(datetime);
  }
}
