package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;

import java.sql.SQLException;
import java.util.List;

@FunctionParseNode.BuiltInFunction(
        name = HllUnionFunction.NAME,
        args = {
                @FunctionParseNode.Argument(allowedTypes = {PVarbinary.class}),
                @FunctionParseNode.Argument(allowedTypes = {PVarbinary.class})
        }
)
public class HllUnionFunction extends ScalarFunction {

    public static final String NAME = "HLL_UNION";

    public HllUnionFunction() throws SQLException {

    }

    public HllUnionFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg1Expr = children.get(0);
        if (!arg1Expr.evaluate(tuple, ptr)) return false;
        if (ptr.getLength() == 0) return true;
        byte[] arg1 = HyperLogLog.decompress(ByteUtil.copyKeyBytesIfNecessary(ptr), 0);

        Expression arg2Expr = children.get(1);
        if (!arg2Expr.evaluate(tuple, ptr)) return false;
        if (ptr.getLength() == 0) return true;
        byte[] arg2 = ByteUtil.copyKeyBytesIfNecessary(ptr);

        HyperLogLog.merge(arg1, 0, arg2, 0);
        ptr.set(arg1);

        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
