package org.apache.ignite.math.impls;

import java.util.Map;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.RandomAccessSparseVectorStorage;

/**
 * TODO: add description.
 */
public class RandomAccessSparseLocalOnHeapVector extends AbstractVector {
    /** For serialization */
    public RandomAccessSparseLocalOnHeapVector(){
        // No-op.
    }

    /** */
    public RandomAccessSparseLocalOnHeapVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size"))
            setStorage(new RandomAccessSparseVectorStorage((int) args.get("size")), (int) args.get("size"));
        else if (args.containsKey("arr") && args.containsKey("copy"))
            setStorage(new RandomAccessSparseVectorStorage((double[])args.get("arr"), (boolean)args.get("copy")));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /**
     * Create empty vector with given cardinality(size).
     *
     * @param cols Vector cardinality.
     */
    public RandomAccessSparseLocalOnHeapVector(int cols) {
        super(cols);
        setStorage(new RandomAccessSparseVectorStorage(cols));
    }

    /** */
    private RandomAccessSparseLocalOnHeapVector(RandomAccessSparseLocalOnHeapVector vector) {
        super(vector);
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return new RandomAccessSparseLocalOnHeapVector(this);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new RandomAccessSparseLocalOnHeapVector(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return null;
    }
}
