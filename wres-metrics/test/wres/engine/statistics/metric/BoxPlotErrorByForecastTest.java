package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.engine.statistics.metric.BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder;

/**
 * Tests the {@link BoxPlotErrorByForecast}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class BoxPlotErrorByForecastTest
{

    /**
     * Constructs a {@link BoxPlotErrorByForecast} and compares the actual output against the expected output when
     * plotting against the ensemble mean forecast value.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1BoxPlotErrorByEnsembleMean() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory dataF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = dataF.getMetadataFactory();

        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( dataF.pairOf( 0.0, new double[] { 0.0, 20.0, 30.0, 50.0, 100.0 } ) );
        final MetadataFactory metFac = dataF.getMetadataFactory();
        final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                 Instant.MAX,
                                                 ReferenceTime.VALID_TIME,
                                                 24 );
        final Metadata meta = metFac.getMetadata( metFac.getDimension( "MM/DAY" ),
                                                  metFac.getDatasetIdentifier( "A", "MAP" ),
                                                  window );
        EnsemblePairs input = dataF.ofEnsemblePairs( values, meta );
        //Build the metric
        final BoxPlotErrorByForecastBuilder b = new BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder();
        b.setOutputFactory( dataF );
        b.setDomainDimension( MetricDimension.ENSEMBLE_MEAN );
        b.setProbabilities( dataF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ) );
        BoxPlotErrorByForecast bpe = (BoxPlotErrorByForecast) b.build();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                                                   MetricConstants.MAIN,
                                                                   metFac.getDatasetIdentifier( "A", "MAP" ),
                                                                   window );

        //Compute normally
        final BoxPlotOutput actual = bpe.apply( input );
        final PairOfDoubleAndVectorOfDoubles expectedBox =
                dataF.pairOf( 40.0, new double[] { 0.0, 10, 30.0, 75.0, 100.0 } );
        List<PairOfDoubleAndVectorOfDoubles> expectedBoxes = new ArrayList<>();
        expectedBoxes.add( expectedBox );
        BoxPlotOutput expected = dataF.ofBoxPlotOutput( expectedBoxes,
                                                        dataF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ),
                                                        m1,
                                                        MetricDimension.ENSEMBLE_MEAN,
                                                        MetricDimension.FORECAST_ERROR );
        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.", actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for box plot of errors by observed value.",
                    bpe.getName().equals( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE.toString() ) );
        assertTrue( "Box plot of errors by observed value has real units", bpe.hasRealUnits() );
    }
    
    /**
     * Constructs a {@link BoxPlotErrorByForecast} and compares the actual output against the expected output when
     * plotting against the ensemble median forecast value.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test2BoxPlotErrorByEnsembleMedian() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory dataF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = dataF.getMetadataFactory();

        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( dataF.pairOf( 0.0, new double[] { 0.0, 20.0, 30.0, 50.0, 100.0 } ) );
        final MetadataFactory metFac = dataF.getMetadataFactory();
        final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                 Instant.MAX,
                                                 ReferenceTime.VALID_TIME,
                                                 24 );
        final Metadata meta = metFac.getMetadata( metFac.getDimension( "MM/DAY" ),
                                                  metFac.getDatasetIdentifier( "A", "MAP" ),
                                                  window );
        EnsemblePairs input = dataF.ofEnsemblePairs( values, meta );
        //Build the metric
        final BoxPlotErrorByForecastBuilder b = new BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder();
        b.setOutputFactory( dataF );
        b.setDomainDimension( MetricDimension.ENSEMBLE_MEDIAN );
        b.setProbabilities( dataF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ) );
        BoxPlotErrorByForecast bpe = (BoxPlotErrorByForecast) b.build();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                                                   MetricConstants.MAIN,
                                                                   metFac.getDatasetIdentifier( "A", "MAP" ),
                                                                   window );

        //Compute normally
        final BoxPlotOutput actual = bpe.apply( input );
        final PairOfDoubleAndVectorOfDoubles expectedBox =
                dataF.pairOf( 30.0, new double[] { 0.0, 10, 30.0, 75.0, 100.0  } );
        List<PairOfDoubleAndVectorOfDoubles> expectedBoxes = new ArrayList<>();
        expectedBoxes.add( expectedBox );
        BoxPlotOutput expected = dataF.ofBoxPlotOutput( expectedBoxes,
                                                        dataF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ),
                                                        m1,
                                                        MetricDimension.ENSEMBLE_MEDIAN,
                                                        MetricDimension.FORECAST_ERROR );
        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.", actual.equals( expected ) );
    }    

    /**
     * Constructs a {@link BoxPlotErrorByForecast} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test3Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final BoxPlotErrorByForecastBuilder b = new BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder();
        b.setOutputFactory( outF );

        //Test for construction with a null domain dimension
        try
        {
            b.setDomainDimension( null );
            b.build();
            fail( "Expected an exception on a null dimension for the domain axis." );
        }
        catch ( MetricParameterException e )
        {
        }
    }


}
