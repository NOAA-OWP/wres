package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link VectorOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class VectorOutputTest
{

    /**
     * Constructs a {@link VectorOutput} and tests for equality with another {@link VectorOutput}.
     */

    @Test
    public void test1Equals()
    {
        final MetricOutputFactory d = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "A",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata(11,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "A",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "B",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final VectorOutput s = d.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        final VectorOutput t = d.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        assertTrue("Expected equal outputs.", s.equals(t));
        assertTrue("Expected non-equal outputs.", !s.equals(null));
        assertTrue("Expected non-equal outputs.", !s.equals(new Double(1.0)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofVectorOutput(new double[]{2.0, 10}, m1)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofVectorOutput(new double[]{2.0, 10}, m2)));
        final VectorOutput q = d.ofVectorOutput(new double[]{1.0, 1.0}, m2);
        final VectorOutput r = d.ofVectorOutput(new double[]{1.0, 1.0}, m3);
        assertTrue("Expected equal outputs.", q.equals(q));
        assertTrue("Expected non-equal outputs.", !s.equals(q));
        assertTrue("Expected non-equal outputs.", !q.equals(s));
        assertTrue("Expected non-equal outputs.", !q.equals(r));
    }

    /**
     * Constructs a {@link VectorOutput} and checks the {@link VectorOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final MetricOutputFactory d = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "A",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final VectorOutput s = d.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        final VectorOutput t = d.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        assertTrue("Expected equal string representations.", s.toString().equals(t.toString()));
    }

    /**
     * Constructs a {@link VectorOutput} and checks the {@link VectorOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final MetricOutputFactory d = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "A",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "B",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final VectorOutput q = d.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        final VectorOutput r = d.ofVectorOutput(new double[]{1.0, 1.0}, m2);
        assertTrue("Expected unequal dimensions.", !q.getMetadata().equals(r.getMetadata()));
    }

    /**
     * Constructs a {@link VectorOutput} and checks the {@link VectorOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final MetricOutputFactory d = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "A",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "A",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "B",
                                                                  "B",
                                                                  "C",
                                                                  null);
        final VectorOutput q = d.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        final VectorOutput r = d.ofVectorOutput(new double[]{1.0, 1.0}, m2);
        assertTrue("Expected equal hash codes.", q.hashCode() == r.hashCode());
        assertTrue("Expected unequal hash codes.", q.hashCode() != d.ofVectorOutput(new double[]{1.0}, m3).hashCode());
    }

}
