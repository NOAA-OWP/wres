package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

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
        final MetricOutputFactory d = DefaultMetricOutputFactory.of();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getMetadata(10,
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "A",
                                                            null);
        final MetricOutputMetadata m2 = metaFac.getMetadata(11,
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "A",
                                                            null);
        final MetricOutputMetadata m3 = metaFac.getMetadata(10,
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "B",
                                                            null);

        final MatrixOutput s = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput t = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        assertTrue("Expected equal outputs.", s.equals(t));
        assertTrue("Expected non-equal outputs.", !s.equals(null));
        assertTrue("Expected non-equal outputs.", !s.equals(new Double(1.0)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofMatrixOutput(new double[][]{{2.0}, {1.0}}, m1)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofMatrixOutput(new double[][]{{2.0}, {1.0}}, m2)));
        final MatrixOutput q = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m2);
        final MatrixOutput r = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m3);
        final MatrixOutput u = d.ofMatrixOutput(new double[][]{{1.0, 1.0}}, m3);
        final MatrixOutput v = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}, {1.0}}, m3);
        final MatrixOutput w = d.ofMatrixOutput(new double[][]{{1.0, 1.0}, {1.0, 1.0}}, m3);
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
        final MetricOutputFactory d = DefaultMetricOutputFactory.of();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getMetadata(10,
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "A",
                                                            null);
        final MatrixOutput s = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput t = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        assertTrue("Expected equal string representations.", s.toString().equals(t.toString()));
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final MetricOutputFactory d = DefaultMetricOutputFactory.of();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = d.getMetadataFactory().getMetadata(10,
                                                                           metaFac.getDimension(),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           "A",
                                                                           null);
        final MetricOutputMetadata m2 = d.getMetadataFactory().getMetadata(10,
                                                                           metaFac.getDimension(),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           "B",
                                                                           null);
        final MatrixOutput q = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput r = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m2);
        assertTrue("Metadata unequal.", !q.getMetadata().equals(r.getMetadata()));
    }

    /**
     * Constructs a {@link MatrixOutput} and checks the {@link MatrixOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final MetadataFactory metaFac = DefaultMetadataFactory.of();
        final MetricOutputMetadata m1 = metaFac.getMetadata(10,
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "A",
                                                            null);
        final MetricOutputMetadata m2 = metaFac.getMetadata(10,
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "A",
                                                            null);
        final MetricOutputMetadata m3 = metaFac.getMetadata(10,
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "B",
                                                            null);
        final MetricOutputFactory d = DefaultMetricOutputFactory.of();
        final MatrixOutput q = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput r = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m2);
        assertTrue("Expected equal hash codes.", q.hashCode() == r.hashCode());
        assertTrue("Expected unequal hash codes.",
                   q.hashCode() != d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m3).hashCode());
    }

}
