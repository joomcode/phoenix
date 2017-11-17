package org.apache.phoenix.expression.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

        // get HLL from argument and calculate cardinality
        try (ByteArrayInputStream bais = new ByteArrayInputStream(ptr.get(), ptr.getOffset(), ptr.getLength());
             DataInputStream di = new DataInputStream(bais)) {
            HyperLogLogPlus arg = HyperLogLogPlus.Builder.build(di);
            long cardinality = arg.cardinality();

            ptr.set(new byte[getDataType().getByteSize()]);
            getDataType().getCodec().encodeLong(cardinality, ptr);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

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
