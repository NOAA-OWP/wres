package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;

/**
 * Tests the {@link MatrixOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MatrixOutputTest
{

    /**
     * Constructs a {@link MatrixOutput} and tests for equality with another {@link MatrixOutput}.
     */

    @Test
    public void test1Equals()
    {
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "A",
                                                                    null);
        final MetricOutputMetadata m2 = MetadataFactory.getMetadata(11,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "A",
                                                                    null);
        final MetricOutputMetadata m3 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "B",
                                                                    null);

        final MatrixOutput s = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput t = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        assertTrue("Expected equal outputs.", s.equals(t));
        assertTrue("Expected non-equal outputs.", !s.equals(null));
        assertTrue("Expected non-equal outputs.", !s.equals(new Double(1.0)));
        assertTrue("Expected non-equal outputs.",
                   !s.equals(MetricOutputFactory.ofMatrixOutput(new double[][]{{2.0}, {1.0}}, m1)));
        assertTrue("Expected non-equal outputs.",
                   !s.equals(MetricOutputFactory.ofMatrixOutput(new double[][]{{2.0}, {1.0}}, m2)));
        final MatrixOutput q = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m2);
        final MatrixOutput r = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m3);
        final MatrixOutput u = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0, 1.0}}, m3);
        final MatrixOutput v = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}, {1.0}}, m3);
        final MatrixOutput w = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0, 1.0}, {1.0, 1.0}}, m3);
        assertTrue("Expected equal outputs.", q.equals(q));
        assertTrue("Expected non-equal outputs.", !s.equals(q));
        assertTrue("Expected non-equal outputs.", !q.equals(s));
        assertTrue("Expected non-equal outputs.", !q.equals(r));
        assertTrue("Expected non-equal outputs.", !r.equals(u));
        assertTrue("Expected non-equal outputs.", !r.equals(v));
        assertTrue("Expected non-equal outputs.", !r.equals(w));
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "A",
                                                                    null);
        final MatrixOutput s = new MatrixOutput(DataFactory.matrixOf(new double[][]{{1.0}, {1.0}}), m1);
        final MatrixOutput t = new MatrixOutput(DataFactory.matrixOf(new double[][]{{1.0}, {1.0}}), m1);
        assertTrue("Expected equal string representations.", s.toString().equals(t.toString()));
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "A",
                                                                    null);
        final MetricOutputMetadata m2 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "B",
                                                                    null);
        final MatrixOutput q = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput r = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m2);
        assertTrue("Metadata unequal.", !q.getMetadata().equals(r.getMetadata()));
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "A",
                                                                    null);
        final MetricOutputMetadata m2 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "A",
                                                                    null);
        final MetricOutputMetadata m3 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "B",
                                                                    null);
        final MatrixOutput q = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput r = MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m2);
        assertTrue("Expected equal hash codes.", q.hashCode() == r.hashCode());
        assertTrue("Expected unequal hash codes.",
                   q.hashCode() != MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m3).hashCode());
    }

}
