package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;

import java.sql.SQLException;
import java.util.List;

@FunctionParseNode.BuiltInFunction(name = HllCardinalityFunction.NAME,
        args = {@FunctionParseNode.Argument(allowedTypes = {PVarbinary.class})})
public class HllCardinalityFunction extends ScalarFunction {

    public static final String NAME = "HLL_CARDINALITY";

    public HllCardinalityFunction() throws SQLException {

    }

    public HllCardinalityFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getChildExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }

        long cardinality = HyperLogLog.cardinality(ptr.get(), ptr.getOffset());
        ptr.set(new byte[getDataType().getByteSize()]);
        getDataType().getCodec().encodeLong(cardinality, ptr);

        return true;
    }

    @Override
    public PDataType getDataType() {
        return PLong.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getChildExpression() {
        return children.get(0);
    }
}
