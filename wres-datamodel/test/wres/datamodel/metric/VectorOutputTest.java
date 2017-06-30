package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;

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
        final VectorOutput s = MetricOutputFactory.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        final VectorOutput t = MetricOutputFactory.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        assertTrue("Expected equal outputs.", s.equals(t));
        assertTrue("Expected non-equal outputs.", !s.equals(null));
        assertTrue("Expected non-equal outputs.", !s.equals(new Double(1.0)));
        assertTrue("Expected non-equal outputs.",
                   !s.equals(MetricOutputFactory.ofVectorOutput(new double[]{2.0, 10}, m1)));
        assertTrue("Expected non-equal outputs.",
                   !s.equals(MetricOutputFactory.ofVectorOutput(new double[]{2.0, 10}, m2)));
        final VectorOutput q = MetricOutputFactory.ofVectorOutput(new double[]{1.0, 1.0}, m2);
        final VectorOutput r = MetricOutputFactory.ofVectorOutput(new double[]{1.0, 1.0}, m3);
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
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(10,
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CONTINGENCY_TABLE,
                                                                    MetricConstants.MAIN,
                                                                    "A",
                                                                    null);
        final VectorOutput s = new VectorOutput(DataFactory.vectorOf(new double[]{1.0, 1.0}), m1);
        final VectorOutput t = new VectorOutput(DataFactory.vectorOf(new double[]{1.0, 1.0}), m1);
        assertTrue("Expected equal string representations.", s.toString().equals(t.toString()));
    }

    /**
     * Constructs a {@link VectorOutput} and checks the {@link VectorOutput#getMetadata()}.
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
        final VectorOutput q = MetricOutputFactory.ofVectorOutput(new double[]{1.0, 1.0}, m1);
        final VectorOutput r = MetricOutputFactory.ofVectorOutput(new double[]{1.0, 1.0}, m2);
        assertTrue("Expected unequal dimensions.", !q.getMetadata().equals(r.getMetadata()));
    }

    /**
     * Constructs a {@link VectorOutput} and checks the {@link VectorOutput#hashCode()}.
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
        final VectorOutput q = MetricOutputFactory.ofVectorOutput(new double[]{1.0, 1.0},m1);
        final VectorOutput r = MetricOutputFactory.ofVectorOutput(new double[]{1.0, 1.0},m2);
        assertTrue("Expected equal hash codes.", q.hashCode() == r.hashCode());
        assertTrue("Expected unequal hash codes.",
                   q.hashCode() != MetricOutputFactory.ofVectorOutput(new double[]{1.0},m3).hashCode());
    }

}
