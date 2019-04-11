package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Tests the {@link BoxPlotErrorByObserved}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BoxPlotErrorByObservedTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link BoxPlotErrorByObserved}.
     */

    private BoxPlotErrorByObserved bpe;

    @Before
    public void setupBeforeEachTest()
    {
        this.bpe = BoxPlotErrorByObserved.of();
    }

    /**
     * Compares the output from {@link BoxPlotErrorByObserved#apply(EnsemblePairs)} against 
     * expected output.
     */

    @Test
    public void testApply()
    {
        List<EnsemblePair> values = new ArrayList<>();
        values.add( EnsemblePair.of( 50.0, new double[] { 0.0, 25.0, 50.0, 75.0, 100.0 } ) );

        TimeWindow window = TimeWindow.of( Instant.MIN,
                                           Instant.MAX,
                                           Duration.ofHours( 24 ) );
        final TimeWindow timeWindow1 = window;
        SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "MM/DAY" ) )
                                                         .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                               "MAP" ) )
                                                         .setTimeWindow( timeWindow1 )
                                                         .build();

        EnsemblePairs input = EnsemblePairs.of( values, meta );
        final TimeWindow timeWindow = window;

        final StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "MM/DAY" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                       "MAP" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of( "MM/DAY" ),
                                      MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                      MetricConstants.MAIN );

        //Compute normally
        final BoxPlotStatistics actual = bpe.apply( input );

        final BoxPlotStatistic expectedBox =
                BoxPlotStatistic.of( VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 ),
                                     VectorOfDoubles.of( -50.0, -37.5, 0.0, 37.5, 50.0 ),
                                     m1,
                                     50.0 );
        
        List<BoxPlotStatistic> expectedBoxes = Collections.singletonList( expectedBox );
        
        BoxPlotStatistics expected = BoxPlotStatistics.of( expectedBoxes,
                                                           MetricDimension.OBSERVED_VALUE,
                                                           MetricDimension.FORECAST_ERROR,
                                                           m1 );

        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link BoxPlotErrorByObserved#apply(EnsemblePairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        EnsemblePairs input =
                EnsemblePairs.of( Arrays.asList(), SampleMetadata.of() );

        BoxPlotStatistics actual = bpe.apply( input );

        assertTrue( actual.getData().equals( Arrays.asList() ) );
    }

    /**
     * Checks that the {@link BoxPlotErrorByObserved#getName()} returns 
     * {@link MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( bpe.getName().equals( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE.toString() ) );
    }

    /**
     * Checks that the {@link BoxPlotErrorByObserved#hasRealUnits()} returns <code>true</code>.
     */

    @Test
    public void testHasRealUnits()
    {
        assertTrue( bpe.hasRealUnits() );
    }

    /**
     * Tests the construction of a {@link BoxPlotErrorByObserved} with two probabilities.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testConstructionWithTwoProbabilities() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( BoxPlotErrorByObserved.of( VectorOfDoubles.of( 0.0, 1.0 ) ) ) );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link BoxPlotErrorByObserved#apply(EnsemblePairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'BOX PLOT OF ERRORS BY OBSERVED VALUE'." );

        bpe.apply( null );
    }

    /**
     * Tests for an expected exception on construction with too few probabilities. 
     */

    @Test
    public void testForExceptionOnTooFewProbabilities() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify at least two probabilities for the verification box plot." );

        BoxPlotErrorByObserved.of( VectorOfDoubles.of( 0.1 ) );
    }

    /**
     * Tests for an expected exception on construction with negative probabilities. 
     */

    @Test
    public void testForExceptionOnNegativeProbabilities() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify only valid probabilities within [0,1] from which to construct the box plot." );

        BoxPlotErrorByObserved.of( VectorOfDoubles.of( -0.1, 0.0, 0.5 ) );
    }

    /**
     * Tests for an expected exception on construction with probabilities that are too high. 
     */

    @Test
    public void testForExceptionOnProbabilitiesGreaterThanOne() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify only valid probabilities within [0,1] from which to construct the box plot." );

        BoxPlotErrorByObserved.of( VectorOfDoubles.of( 0.0, 0.5, 1.5 ) );
    }

    /**
     * Tests for an expected exception on construction with duplicate probabilities. 
     */

    @Test
    public void testForExceptionOnDuplicateProbabilities() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify only non-unique probabilities from which to construct the box plot." );

        BoxPlotErrorByObserved.of( VectorOfDoubles.of( 0.0, 0.0, 1.0 ) );
    }

    /**
     * Tests for an expected exception on construction with an even number of probabilities. 
     */

    @Test
    public void testForExceptionOnEvenNumberOfProbabilities() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify an odd number of probabilities for the verification box plot." );

        BoxPlotErrorByObserved.of( VectorOfDoubles.of( 0.0, 0.25, 0.5, 1.0 ) );
    }

}
