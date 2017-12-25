package wres.datamodel;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;

/**
 * Tests the {@link SafeMatrixOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeMatrixOutputTest
{

    /**
     * Constructs a {@link SafeMatrixOutput} and tests for equality with another {@link SafeMatrixOutput}.
     */

    @Test
    public void test1Equals()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("A", "B", "C"));
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata(11,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("A", "B", "C"));
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("B", "B", "C"));
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
     * Constructs a {@link SafeMatrixOutput} and checks the {@link SafeMatrixOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("A", "B", "C"));
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
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = d.getMetadataFactory().getOutputMetadata(10,
                                                                                 metaFac.getDimension(),
                                                                                 metaFac.getDimension("CMS"),
                                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                                 MetricConstants.MAIN,
                                                                                 metaFac.getDatasetIdentifier("A", "B", "C"));
        final MetricOutputMetadata m2 = d.getMetadataFactory().getOutputMetadata(10,
                                                                                 metaFac.getDimension(),
                                                                                 metaFac.getDimension("CMS"),
                                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                                 MetricConstants.MAIN,
                                                                                 metaFac.getDatasetIdentifier("B", "B", "C"));
        final MatrixOutput q = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput r = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m2);
        assertTrue("Metadata unequal.", !q.getMetadata().equals(r.getMetadata()));
    }

    /**
     * Constructs a {@link SafeMatrixOutput} and checks the {@link SafeMatrixOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("A", "B", "C"));
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("A", "B", "C"));
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier("B", "B", "C"));
        final DataFactory d = DefaultDataFactory.getInstance();
        final MatrixOutput q = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m1);
        final MatrixOutput r = d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m2);
        assertTrue("Expected equal hash codes.", q.hashCode() == r.hashCode());
        assertTrue("Expected unequal hash codes.",
                   q.hashCode() != d.ofMatrixOutput(new double[][]{{1.0}, {1.0}}, m3).hashCode());
    }

}
