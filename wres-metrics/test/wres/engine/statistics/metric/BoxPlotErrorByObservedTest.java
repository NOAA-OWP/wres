package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.BoxPlotOutput;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.Metadata;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.ReferenceTime;
import wres.datamodel.TimeWindow;
import wres.engine.statistics.metric.BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder;

/**
 * Tests the {@link BoxPlotErrorByObserved}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class BoxPlotErrorByObservedTest
{

    /**
     * Constructs a {@link BoxPlotErrorByObserved} and compares the actual output against the expected output.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1BoxPlotErrorByObserved() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory dataF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = dataF.getMetadataFactory();

        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( dataF.pairOf( 50.0, new double[] { 0.0, 25.0, 50.0, 75.0, 100.0 } ) );
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
        final BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( dataF );
        //Build with and without explicit probabilities
        final BoxPlotErrorByObserved bpe = (BoxPlotErrorByObserved) b.build();
        b.setProbabilities( dataF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ) );
        b.build();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED,
                                                                   MetricConstants.MAIN,
                                                                   metFac.getDatasetIdentifier( "A", "MAP" ),
                                                                   window );

        //Compute normally
        final BoxPlotOutput actual = bpe.apply( input );
        final PairOfDoubleAndVectorOfDoubles expectedBox =
                dataF.pairOf( 50.0, new double[] { -50.0, -37.5, 0.0, 37.5, 50.0 } );
        List<PairOfDoubleAndVectorOfDoubles> expectedBoxes = new ArrayList<>();
        expectedBoxes.add( expectedBox );
        BoxPlotOutput expected = dataF.ofBoxPlotOutput( expectedBoxes,
                                                        dataF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ),
                                                        m1,
                                                        MetricDimension.OBSERVED_VALUE,
                                                        MetricDimension.FORECAST_ERROR );     
        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.", actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for box plot of errors by observed value.",
                    bpe.getName().equals( metaFac.getMetricName( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED ) ) );
        assertTrue( "Box plot of errors by observed value has real units", bpe.hasRealUnits() );
    }

    /**
     * Constructs a {@link BoxPlotErrorByObserved} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( outF );
        final BoxPlotErrorByObserved bp = (BoxPlotErrorByObserved) b.build();

        //Check for null input
        try
        {
            bp.apply( (EnsemblePairs) null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
        //Test for construction with an insufficient number of probabilities
        try
        {
            b.setProbabilities( outF.vectorOf( new double[] { 0.1 } ) );
            b.build();
            fail( "Expected an exception on insufficient probabiilty thresholds." );
            //Reset
            b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
                                                              1.0 } ) );
        }
        catch ( MetricParameterException e )
        {
        }
        //Test for construction with OOB probabilities low
        try
        {
            b.setProbabilities( outF.vectorOf( new double[] { -0.1, 0.0, 0.5 } ) );
            b.build();
            fail( "Expected an exception on probabiity thresholds < 0.0." );
        }
        catch ( MetricParameterException e )
        {
        }
        //Test for construction with OOB probabilities high
        try
        {
            b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.5, 1.5 } ) );
            b.build();
            fail( "Expected an exception on probabiity thresholds > 1.0." );
        }
        catch ( MetricParameterException e )
        {
        }
        //Test for construction with duplicate probabilities
        try
        {
            b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.0, 1.0 } ) );
            b.build();
            fail( "Expected an exception on duplicate probabiity thresholds." );
        }
        catch ( MetricParameterException e )
        {
        }
        //Test for construction with an even number of probabilities
        try
        {
            b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.25, 0.5, 1.0 } ) );
            b.build();
            fail( "Expected an exception on an even number of probabiity thresholds." );
        }
        catch ( MetricParameterException e )
        {
        }
    }


}
