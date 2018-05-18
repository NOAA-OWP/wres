package wres.datamodel;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.ScoreOutput;

/**
 * Tests the {@link SafeDoubleScoreOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeScalarOutputTest
{

    /**
     * Constructs a {@link SafeDoubleScoreOutput} and tests for equality with another {@link SafeDoubleScoreOutput}.
     */

    @Test
    public void test1Equals()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l1, "B", "C"));
        final Location l2 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata(11,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l2, "B", "C"));
        final Location l3 = metaFac.getLocation( "B" );
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l3, "B", "C"));
        final ScoreOutput s = d.ofDoubleScoreOutput(1.0, m1);
        final ScoreOutput t = d.ofDoubleScoreOutput(1.0, m1);
        assertTrue("Expected equal outputs.", s.equals(t));
        assertTrue("Expected non-equal outputs.", !s.equals(null));
        assertTrue("Expected non-equal outputs.", !s.equals(new Double(1.0)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofDoubleScoreOutput(2.0, m1)));
        assertTrue("Expected non-equal outputs.", !s.equals(d.ofDoubleScoreOutput(1.0, m2)));
        final ScoreOutput q = d.ofDoubleScoreOutput(1.0, m2);
        final ScoreOutput r = d.ofDoubleScoreOutput(1.0, m3);
        assertTrue("Expected non-equal outputs.", !s.equals(q));
        assertTrue("Expected equal outputs.", q.equals(q));
        assertTrue("Expected non-equal outputs.", !q.equals(s));
        assertTrue("Expected non-equal outputs.", !q.equals(r));
    }

    /**
     * Constructs a {@link SafeDoubleScoreOutput} and checks the {@link SafeDoubleScoreOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l1, "B", "C"));
        final ScoreOutput s = d.ofDoubleScoreOutput(1.0, m1);
        final ScoreOutput t = d.ofDoubleScoreOutput(1.0, m1);
        assertTrue("Expected equal string representations.", s.toString().equals(t.toString()));
    }

    /**
     * Constructs a {@link SafeDoubleScoreOutput} and checks the {@link SafeDoubleScoreOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l1, "B", "C"));
        final Location l2 = metaFac.getLocation( "B" );
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l2, "B", "C"));
        final ScoreOutput q = d.ofDoubleScoreOutput(1.0, m1);
        final ScoreOutput r = d.ofDoubleScoreOutput(1.0, m2);
        assertTrue("Unequal metadata.", !q.getMetadata().equals(r.getMetadata()));
    }

    /**
     * Constructs a {@link SafeDoubleScoreOutput} and checks the {@link SafeDoubleScoreOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l1, "B", "C"));
        final Location l2 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l2, "B", "C"));
        final Location l3 = metaFac.getLocation( "B" );
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata(10,
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  metaFac.getDatasetIdentifier(l3, "B", "C"));
        final ScoreOutput q = d.ofDoubleScoreOutput(1.0, m1);
        final ScoreOutput r = d.ofDoubleScoreOutput(1.0, m2);
        assertTrue("Expected equal hash codes.", q.hashCode() == r.hashCode());
        assertTrue("Expected unequal hash codes.", q.hashCode() != d.ofDoubleScoreOutput(1.0, m3).hashCode());
    }

}
