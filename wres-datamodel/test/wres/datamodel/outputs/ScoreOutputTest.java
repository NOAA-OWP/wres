package wres.datamodel.outputs;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * Tests the {@link ScoreOutput}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class ScoreOutputTest
{

    /**
     * Constructs a {@link DoubleScoreOutput} and tests for equality with another {@link DoubleScoreOutput}.
     */

    @SuppressWarnings( "unlikely-arg-type" )
    @Test
    public void test1Equals()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l2 = Location.of( "A" );
        final MetricOutputMetadata m2 = MetricOutputMetadata.of( 11,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l2,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l3 = Location.of( "B" );
        final MetricOutputMetadata m3 = MetricOutputMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l3,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final ScoreOutput<Double, DoubleScoreOutput> s = DoubleScoreOutput.of( 1.0, m1 );
        final ScoreOutput<Double, DoubleScoreOutput> t = DoubleScoreOutput.of( 1.0, m1 );
        assertTrue( "Expected equal outputs.", s.equals( t ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( null ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( new Double( 1.0 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( DoubleScoreOutput.of( 2.0, m1 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( DoubleScoreOutput.of( 1.0, m2 ) ) );
        final ScoreOutput<Double, DoubleScoreOutput> q = DoubleScoreOutput.of( 1.0, m2 );
        final ScoreOutput<Double, DoubleScoreOutput> r = DoubleScoreOutput.of( 1.0, m3 );
        assertTrue( "Expected non-equal outputs.", !s.equals( q ) );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( s ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( r ) );
    }

    /**
     * Constructs a {@link DoubleScoreOutput} and checks the {@link DoubleScoreOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final ScoreOutput<Double, DoubleScoreOutput> s = DoubleScoreOutput.of( 1.0, m1 );
        final ScoreOutput<Double, DoubleScoreOutput> t = DoubleScoreOutput.of( 1.0, m1 );
        assertTrue( "Expected equal string representations.", s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link DoubleScoreOutput} and checks the {@link DoubleScoreOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l2 = Location.of( "B" );
        final MetricOutputMetadata m2 = MetricOutputMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l2,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final ScoreOutput<Double, DoubleScoreOutput> q = DoubleScoreOutput.of( 1.0, m1 );
        final ScoreOutput<Double, DoubleScoreOutput> r = DoubleScoreOutput.of( 1.0, m2 );
        assertTrue( "Unequal metadata.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link DoubleScoreOutput} and checks the {@link DoubleScoreOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l1,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l2 = Location.of( "A" );
        final MetricOutputMetadata m2 = MetricOutputMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l2,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final Location l3 = Location.of( "B" );
        final MetricOutputMetadata m3 = MetricOutputMetadata.of( 10,
                                                                           MeasurementUnit.of(),
                                                                           MeasurementUnit.of( "CMS" ),
                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                           MetricConstants.MAIN,
                                                                           DatasetIdentifier.of( l3,
                                                                                                                 "B",
                                                                                                                 "C" ) );
        final ScoreOutput<Double, DoubleScoreOutput> q = DoubleScoreOutput.of( 1.0, m1 );
        final ScoreOutput<Double, DoubleScoreOutput> r = DoubleScoreOutput.of( 1.0, m2 );
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected unequal hash codes.",
                    q.hashCode() != DoubleScoreOutput.of( 1.0, m3 ).hashCode() );
    }

}
