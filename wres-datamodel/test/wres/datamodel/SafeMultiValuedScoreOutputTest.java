package wres.datamodel;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MultiValuedScoreOutput;

/**
 * Tests the {@link SafeMultiValuedScoreOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeMultiValuedScoreOutputTest
{

    /**
     * Constructs a {@link SafeMultiValuedScoreOutput} and tests for equality with another {@link SafeMultiValuedScoreOutput}.
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
        final MultiValuedScoreOutput s = d.ofMultiValuedScoreOutput(new double[]{1.0}, m1);
        final MultiValuedScoreOutput t = d.ofMultiValuedScoreOutput(new double[]{1.0}, m1);
        assertTrue("Expected equal outputs.", s.equals(t));
        assertTrue("Expected non-equal outputs.", !s.equals(null));
        assertTrue("Expected non-equal outputs.", !s.equals(new Double(1.0)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofMultiValuedScoreOutput(new double[]{2.0}, m1)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofMultiValuedScoreOutput(new double[]{2.0}, m2)));
        final MultiValuedScoreOutput q = d.ofMultiValuedScoreOutput(new double[]{1.0}, m2);
        final MultiValuedScoreOutput r = d.ofMultiValuedScoreOutput(new double[]{1.0}, m3);
        assertTrue("Expected equal outputs.", q.equals(q));
        assertTrue("Expected non-equal outputs.", !s.equals(q));
        assertTrue("Expected non-equal outputs.", !q.equals(s));
        assertTrue("Expected non-equal outputs.", !q.equals(r));
    }

    /**
     * Constructs a {@link SafeMultiValuedScoreOutput} and checks the {@link SafeMultiValuedScoreOutput#toString()} representation.
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
        final MultiValuedScoreOutput s = d.ofMultiValuedScoreOutput(new double[]{1.0}, m1);
        final MultiValuedScoreOutput t = d.ofMultiValuedScoreOutput(new double[]{1.0}, m1);
        assertTrue("Expected equal string representations.", s.toString().equals(t.toString()));
    }

    /**
     * Constructs a {@link SafeMultiValuedScoreOutput} and checks the {@link SafeMultiValuedScoreOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
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
                                                                  metaFac.getDatasetIdentifier("B", "B", "C"));
        final MultiValuedScoreOutput q = d.ofMultiValuedScoreOutput(new double[]{1.0}, m1);
        final MultiValuedScoreOutput r = d.ofMultiValuedScoreOutput(new double[]{1.0}, m2);
        assertTrue("Expected unequal dimensions.", !q.getMetadata().equals(r.getMetadata()));
    }

    /**
     * Constructs a {@link SafeMultiValuedScoreOutput} and checks the {@link SafeMultiValuedScoreOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
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
        final MultiValuedScoreOutput q = d.ofMultiValuedScoreOutput(new double[]{1.0}, m1);
        final MultiValuedScoreOutput r = d.ofMultiValuedScoreOutput(new double[]{1.0}, m2);
        assertTrue("Expected equal hash codes.", q.hashCode() == r.hashCode());
        assertTrue("Expected unequal hash codes.", q.hashCode() != d.ofMultiValuedScoreOutput(new double[]{1.0}, m3).hashCode());
    }

}
