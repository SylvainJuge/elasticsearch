/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasables;

/**
 * Filter vector for $Type$Vectors.
 * This class is generated. Do not edit it.
 */
public final class Filter$Type$Vector extends AbstractFilterVector implements $Type$Vector {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Filter$Type$Vector.class);

    private final $Type$Vector vector;

    Filter$Type$Vector($Type$Vector vector, int... positions) {
        super(positions, vector.blockFactory());
        this.vector = vector;
    }

    @Override
$if(BytesRef)$
    public BytesRef getBytesRef(int position, BytesRef dest) {
        return vector.getBytesRef(mapPosition(position), dest);
$else$
    public $type$ get$Type$(int position) {
        return vector.get$Type$(mapPosition(position));
$endif$
    }

    @Override
    public $Type$Block asBlock() {
        return new $Type$VectorBlock(this);
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public boolean isConstant() {
        return vector.isConstant();
    }

    @Override
    public $Type$Vector filter(int... positions) {
        return new Filter$Type$Vector(this, positions);
    }

    @Override
    public long ramBytesUsed() {
        // from a usage and resource point of view filter vectors encapsulate
        // their inner vector, rather than listing it as a child resource
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(vector) + RamUsageEstimator.sizeOf(positions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof $Type$Vector that) {
            return $Type$Vector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return $Type$Vector.hash(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        sb.append("[positions=" + getPositionCount() + ", values=[");
        appendValues(sb);
        sb.append("]]");
        return sb.toString();
    }

    private void appendValues(StringBuilder sb) {
        final int positions = getPositionCount();
        for (int i = 0; i < positions; i++) {
            if (i > 0) {
                sb.append(", ");
            }
$if(BytesRef)$
            sb.append(get$Type$(i, new BytesRef()));
$else$
            sb.append(get$Type$(i));
$endif$
        }
    }

    @Override
    public BlockFactory blockFactory() {
        return vector.blockFactory();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(vector);
    }
}
