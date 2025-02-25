// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link MvMedian}.
 * This class is generated. Do not edit it.
 */
public final class MvMedianDoubleEvaluator extends AbstractMultivalueFunction.AbstractEvaluator {
  private final DriverContext driverContext;

  public MvMedianDoubleEvaluator(EvalOperator.ExpressionEvaluator field,
      DriverContext driverContext) {
    super(field);
    this.driverContext = driverContext;
  }

  @Override
  public String name() {
    return "MvMedian";
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block.Ref evalNullable(Block.Ref ref) {
    try (ref) {
      DoubleBlock v = (DoubleBlock) ref.block();
      int positionCount = v.getPositionCount();
      DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(positionCount, driverContext.blockFactory());
      MvMedian.Doubles work = new MvMedian.Doubles();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        if (valueCount == 0) {
          builder.appendNull();
          continue;
        }
        int first = v.getFirstValueIndex(p);
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          double value = v.getDouble(i);
          MvMedian.process(work, value);
        }
        double result = MvMedian.finish(work);
        builder.appendDouble(result);
      }
      return Block.Ref.floating(builder.build());
    }
  }

  /**
   * Evaluate blocks containing at least one multivalued field.
   */
  @Override
  public Block.Ref evalNotNullable(Block.Ref ref) {
    try (ref) {
      DoubleBlock v = (DoubleBlock) ref.block();
      int positionCount = v.getPositionCount();
      DoubleVector.FixedBuilder builder = DoubleVector.newVectorFixedBuilder(positionCount, driverContext.blockFactory());
      MvMedian.Doubles work = new MvMedian.Doubles();
      for (int p = 0; p < positionCount; p++) {
        int valueCount = v.getValueCount(p);
        int first = v.getFirstValueIndex(p);
        int end = first + valueCount;
        for (int i = first; i < end; i++) {
          double value = v.getDouble(i);
          MvMedian.process(work, value);
        }
        double result = MvMedian.finish(work);
        builder.appendDouble(result);
      }
      return Block.Ref.floating(builder.build().asBlock());
    }
  }
}
