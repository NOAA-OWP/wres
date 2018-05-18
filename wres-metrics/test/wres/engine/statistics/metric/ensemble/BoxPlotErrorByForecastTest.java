package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder;

/**
 * Tests the {@link BoxPlotErrorByForecast}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BoxPlotErrorByForecastTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link BoxPlotErrorByForecast}.
     */

    private BoxPlotErrorByForecast bpe;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        BoxPlotErrorByForecastBuilder b = new BoxPlotErrorByForecastBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        b.setDomainDimension( MetricDimension.ENSEMBLE_MEAN );
        b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ) );
        this.bpe = (BoxPlotErrorByForecast) b.build();
    }

    /**
     * Compares the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} against 
     * expected output for box plots configured to use the ensemble mean as the forecast value.
     */

    @Test
    public void testApplyWithEnsembleMean()
    {
        //Obtain the factories
        MetadataFactory metaFac = outF.getMetadataFactory();

        List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( outF.pairOf( 0.0, new double[] { 0.0, 20.0, 30.0, 50.0, 100.0 } ) );
        MetadataFactory metFac = outF.getMetadataFactory();
        TimeWindow window = TimeWindow.of( Instant.MIN,
                                           Instant.MAX,
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 24 ) );
        final Metadata meta = metFac.getMetadata( metFac.getDimension( "MM/DAY" ),
                                                  metFac.getDatasetIdentifier( metaFac.getLocation("A"), "MAP" ),
                                                  window );
        EnsemblePairs input = outF.ofEnsemblePairs( values, meta );

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getRawData().size(),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                                                   MetricConstants.MAIN,
                                                                   metFac.getDatasetIdentifier( metaFac.getLocation("A"), "MAP" ),
                                                                   window );

        //Compute normally
        final BoxPlotOutput actual = bpe.apply( input );
        final PairOfDoubleAndVectorOfDoubles expectedBox =
                outF.pairOf( 40.0, new double[] { 0.0, 10, 30.0, 75.0, 100.0 } );
        List<PairOfDoubleAndVectorOfDoubles> expectedBoxes = new ArrayList<>();
        expectedBoxes.add( expectedBox );
        BoxPlotOutput expected = outF.ofBoxPlotOutput( expectedBoxes,
                                                       outF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ),
                                                       m1,
                                                       MetricDimension.ENSEMBLE_MEAN,
                                                       MetricDimension.FORECAST_ERROR );
        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.", actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} against 
     * expected output for box plots configured to use the ensemble median as the forecast value.
     * @throws MetricParameterException if the metric could not be built
     */

    @Test
    public void testApplyWithEnsembleMedian() throws MetricParameterException
    {
        //Obtain the factories
        MetadataFactory metaFac = outF.getMetadataFactory();

        List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( outF.pairOf( 0.0, new double[] { 0.0, 20.0, 30.0, 50.0, 100.0 } ) );
        MetadataFactory metFac = outF.getMetadataFactory();
        TimeWindow window = TimeWindow.of( Instant.MIN,
                                           Instant.MAX,
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 24 ) );
        final Metadata meta = metFac.getMetadata( metFac.getDimension( "MM/DAY" ),
                                                  metFac.getDatasetIdentifier( metaFac.getLocation("A"), "MAP" ),
                                                  window );
        EnsemblePairs input = outF.ofEnsemblePairs( values, meta );

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getRawData().size(),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                                                   MetricConstants.MAIN,
                                                                   metFac.getDatasetIdentifier( metaFac.getLocation("A"), "MAP" ),
                                                                   window );

        //Build the metric
        final BoxPlotErrorByForecastBuilder b = new BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder();
        b.setOutputFactory( outF );
        b.setDomainDimension( MetricDimension.ENSEMBLE_MEDIAN );
        b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ) );
        BoxPlotErrorByForecast bpe = (BoxPlotErrorByForecast) b.build();

        //Compute normally
        final BoxPlotOutput actual = bpe.apply( input );
        final PairOfDoubleAndVectorOfDoubles expectedBox =
                outF.pairOf( 30.0, new double[] { 0.0, 10, 30.0, 75.0, 100.0 } );
        List<PairOfDoubleAndVectorOfDoubles> expectedBoxes = new ArrayList<>();
        expectedBoxes.add( expectedBox );
        BoxPlotOutput expected = outF.ofBoxPlotOutput( expectedBoxes,
                                                       outF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ),
                                                       m1,
                                                       MetricDimension.ENSEMBLE_MEDIAN,
                                                       MetricDimension.FORECAST_ERROR );
        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.", actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        EnsemblePairs input =
                outF.ofEnsemblePairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );

        BoxPlotOutput actual = bpe.apply( input );

        assertTrue( Arrays.equals( actual.getProbabilities().getDoubles(),
                                   new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ) );

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
        exception.expect( MetricInputException.class );
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

        BoxPlotErrorByForecastBuilder b = new BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder();
        b.setOutputFactory( outF );

        //Test for construction with a null domain dimension
        b.setDomainDimension( null );
        b.build();
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

        BoxPlotErrorByForecastBuilder b = new BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder();
        b.setOutputFactory( outF );

        //Test for construction with a null domain dimension
        b.setDomainDimension( MetricDimension.FALSE_NEGATIVES );
        b.build();
    } 
    
}
