package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Tests the {@link BoxPlotErrorByForecast}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BoxPlotErrorByForecastTest
{

    /**
     * Units used in testing.
     */
    
    private static final String MM_DAY = "MM/DAY";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link BoxPlotErrorByForecast}.
     */

    private BoxPlotErrorByForecast bpe;

    @Before
    public void setupBeforeEachTest()
    {
        this.bpe = BoxPlotErrorByForecast.of();
    }

    /**
     * Compares the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} against 
     * expected output for box plots configured to use the ensemble mean as the forecast value.
     */

    @Test
    public void testApplyWithEnsembleMean()
    {
        List<Pair<Double,Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 0.0, 20.0, 30.0, 50.0, 100.0 ) ) );

        TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                           Instant.MAX,
                                           Duration.ofHours( 24 ) );
        final TimeWindowOuter timeWindow1 = window;
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( MM_DAY ) )
                                                               .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                     "MAP" ) )
                                                               .setTimeWindow( timeWindow1 )
                                                               .build();
        SampleData<Pair<Double,Ensemble>> input = SampleDataBasic.of( values, meta );
        final TimeWindowOuter timeWindow = window;

        final StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( MM_DAY ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                       "MAP" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of( MM_DAY ),
                                      MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                      MetricConstants.MAIN );

        //Compute normally
        final BoxPlotStatistics actual = bpe.apply( input );
        final BoxPlotStatistic expectedBox =
                BoxPlotStatistic.of( VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 ),
                                     VectorOfDoubles.of( 0.0, 10, 30.0, 75.0, 100.0 ),
                                     m1,
                                     40.0,
                                     MetricDimension.ENSEMBLE_MEAN );
        
        List<BoxPlotStatistic> expectedBoxes = Collections.singletonList( expectedBox );
        
        BoxPlotStatistics expected = BoxPlotStatistics.of( expectedBoxes, m1 );
        
        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.",
                    actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} against 
     * expected output for box plots configured to use the ensemble median as the forecast value.
     * @throws MetricParameterException if the metric could not be built
     */

    @Test
    public void testApplyWithEnsembleMedian() throws MetricParameterException
    {
        List<Pair<Double,Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 0.0, 20.0, 30.0, 50.0, 100.0 ) ) );

        TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                           Instant.MAX,
                                           Duration.ofHours( 24 ) );
        final TimeWindowOuter timeWindow1 = window;
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( MM_DAY ) )
                                                               .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                     "MAP" ) )
                                                               .setTimeWindow( timeWindow1 )
                                                               .build();
        SampleData<Pair<Double,Ensemble>> input = SampleDataBasic.of( values, meta );
        final TimeWindowOuter timeWindow = window;

        final StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( MM_DAY ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                       "MAP" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of( MM_DAY ),
                                      MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                      MetricConstants.MAIN );

        //Build the metric
        BoxPlotErrorByForecast bpef = BoxPlotErrorByForecast.of( MetricDimension.ENSEMBLE_MEDIAN,
                                                                 VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) );

        //Compute normally
        final BoxPlotStatistics actual = bpef.apply( input );
        final BoxPlotStatistic expectedBox =
                BoxPlotStatistic.of( VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 ),
                                     VectorOfDoubles.of( 0.0, 10, 30.0, 75.0, 100.0 ),
                                     m1,
                                     30.0,
                                     MetricDimension.ENSEMBLE_MEDIAN );
        
        List<BoxPlotStatistic> expectedBoxes = Collections.singletonList( expectedBox );
        
        BoxPlotStatistics expected = BoxPlotStatistics.of( expectedBoxes, m1 );

        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Double,Ensemble>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        BoxPlotStatistics actual = bpe.apply( input );

        assertTrue( actual.getData().equals( Arrays.asList() ) );
    }

    /**
     * Checks that the {@link BoxPlotErrorByForecast#getName()} returns 
     * {@link MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( bpe.getName().equals( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE.toString() ) );
    }

    /**
     * Checks that the {@link BoxPlotErrorByForecast#hasRealUnits()} returns <code>true</code>.
     */

    @Test
    public void testHasRealUnits()
    {
        assertTrue( bpe.hasRealUnits() );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'BOX PLOT OF ERRORS BY FORECAST VALUE'." );

        bpe.apply( null );
    }

    /**
     * Constructs a {@link BoxPlotErrorByForecast} and checks for an expected exception when the forecast dimension 
     * is null. 
     * @throws MetricParameterException if the metric could not be constructed for reasons other than the 
     *            expected reason
     */

    @Test
    public void testConstructionWithNullDimensionException() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot build the box plot of forecast errors by forecast value without a dimension "
                                 + "for the domain axis" );

        BoxPlotErrorByForecast.of( null, VectorOfDoubles.of( 0.0, 1.0 ) );
    }

    /**
     * Constructs a {@link BoxPlotErrorByForecast} and checks for an expected exception when the forecast dimension 
     * is wrong. 
     * @throws MetricParameterException if the metric could not be constructed for reasons other than the 
     *            expected reason
     */

    @Test
    public void testConstructionWithWrongDimensionException() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Unsupported dimension for the domain axis of the box plot: 'FALSE NEGATIVES'." );

        BoxPlotErrorByForecast.of( MetricDimension.FALSE_NEGATIVES, VectorOfDoubles.of( 0.0, 1.0 ) );
    }

}
