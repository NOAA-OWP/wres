package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link ScalarOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ScalarOutputTest
{

    /**
     * Constructs a {@link ScalarOutput} and tests for equality with another {@link ScalarOutput}.
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
        final ScalarOutput s = d.ofScalarOutput(1.0, m1);
        final ScalarOutput t = d.ofScalarOutput(1.0, m1);
        assertTrue("Expected equal outputs.", s.equals(t));
        assertTrue("Expected non-equal outputs.", !s.equals(null));
        assertTrue("Expected non-equal outputs.", !s.equals(new Double(1.0)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofScalarOutput(2.0, m1)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofScalarOutput(1.0, m2)));
        final ScalarOutput q = d.ofScalarOutput(1.0, m2);
        final ScalarOutput r = d.ofScalarOutput(1.0, m3);
        assertTrue("Expected non-equal outputs.", !s.equals(q));
        assertTrue("Expected equal outputs.", q.equals(q));
        assertTrue("Expected non-equal outputs.", !q.equals(s));
        assertTrue("Expected non-equal outputs.", !q.equals(r));
    }

    /**
     * Constructs a {@link ScalarOutput} and checks the {@link ScalarOutput#toString()} representation.
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
        final ScalarOutput s = d.ofScalarOutput(1.0, m1);
        final ScalarOutput t = d.ofScalarOutput(1.0, m1);
        assertTrue("Expected equal string representations.", s.toString().equals(t.toString()));
    }

    /**
     * Constructs a {@link ScalarOutput} and checks the {@link ScalarOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final MetricOutputFactory d = DefaultMetricOutputFactory.of();
        final MetadataFactory metaFac = d.getMetadataFactory();
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
                                                            "B",
                                                            null);
        final ScalarOutput q = d.ofScalarOutput(1.0, m1);
        final ScalarOutput r = d.ofScalarOutput(1.0, m2);
        assertTrue("Unequal metadata.", !q.getMetadata().equals(r.getMetadata()));
    }

    /**
     * Constructs a {@link ScalarOutput} and checks the {@link ScalarOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final MetricOutputFactory d = DefaultMetricOutputFactory.of();
        final MetadataFactory metaFac = d.getMetadataFactory();
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
        final ScalarOutput q = d.ofScalarOutput(1.0, m1);
        final ScalarOutput r = d.ofScalarOutput(1.0, m2);
        assertTrue("Expected equal hash codes.", q.hashCode() == r.hashCode());
        assertTrue("Expected unequal hash codes.", q.hashCode() != d.ofScalarOutput(1.0, m3).hashCode());
    }

}
