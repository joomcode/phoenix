package org.apache.phoenix.expression.function;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.BaseAggregator;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.ByteUtil;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

@FunctionParseNode.BuiltInFunction(
        name = HllMergeAggregateFunction.NAME,
        args = {@FunctionParseNode.Argument(allowedTypes = {PVarbinary.class})}
)
public class HllMergeAggregateFunction extends SingleAggregateFunction {
    public static final String NAME = "HLL_MERGE";

    public static final int NormalSetPrecision = 16;

    public HllMergeAggregateFunction() {
    }

    public HllMergeAggregateFunction(List<Expression> childExpressions){
        super(childExpressions);
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public MergeHllAggregator newClientAggregator() {
        return new MergeHllAggregator(NormalSetPrecision);
    }

    @Override
    public MergeHllAggregator newServerAggregator(Configuration conf) {
        return new MergeHllAggregator(NormalSetPrecision);
    }

    @Override
    public String getName() {
        return NAME;
    }
}

class MergeHllAggregator extends BaseAggregator {
    private final ImmutableBytesWritable valueByteArray = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);

    private final int p;
    private HyperLogLogPlus hll;

    MergeHllAggregator(int p) {
        super(SortOrder.getDefault());
        this.p = p;
        this.hll = new HyperLogLogPlus(p);
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(ptr.get(), ptr.getOffset(), ptr.getLength());
             DataInputStream di = new DataInputStream(bais)) {
            HyperLogLogPlus arg = HyperLogLogPlus.Builder.build(di);
            hll.addAll(arg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        try {
            valueByteArray.set(hll.getBytes(), 0, hll.getBytes().length);
            ptr.set(ByteUtil.copyKeyBytesIfNecessary(valueByteArray));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public void reset() {
        hll = new HyperLogLogPlus(p);
    }

    @Override
    public int getSize() {
        return hll.sizeof();
    }

    @Override
    public PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public String toString() {
        return "HLL_MERGE [hll=" + hll.cardinality() + "]";
    }
}
