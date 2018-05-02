package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder;

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

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObservedBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ) );
        this.bpe = (BoxPlotErrorByObserved) b.build();
    }

    /**
     * Compares the output from {@link BoxPlotErrorByObserved#apply(EnsemblePairs)} against 
     * expected output.
     */

    @Test
    public void testApply()
    {
        final MetadataFactory metaFac = outF.getMetadataFactory();

        List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( outF.pairOf( 50.0, new double[] { 0.0, 25.0, 50.0, 75.0, 100.0 } ) );
        MetadataFactory metFac = outF.getMetadataFactory();
        TimeWindow window = TimeWindow.of( Instant.MIN,
                                           Instant.MAX,
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 24 ) );
        Metadata meta = metFac.getMetadata( metFac.getDimension( "MM/DAY" ),
                                            metFac.getDatasetIdentifier( "A", "MAP" ),
                                            window );

        EnsemblePairs input = outF.ofEnsemblePairs( values, meta );

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getRawData().size(),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   metFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                                   MetricConstants.MAIN,
                                                                   metFac.getDatasetIdentifier( "A", "MAP" ),
                                                                   window );

        //Compute normally
        final BoxPlotOutput actual = bpe.apply( input );
        final PairOfDoubleAndVectorOfDoubles expectedBox =
                outF.pairOf( 50.0, new double[] { -50.0, -37.5, 0.0, 37.5, 50.0 } );
        List<PairOfDoubleAndVectorOfDoubles> expectedBoxes = new ArrayList<>();
        expectedBoxes.add( expectedBox );
        BoxPlotOutput expected = outF.ofBoxPlotOutput( expectedBoxes,
                                                       outF.vectorOf( new double[] { 0.0, 0.25, 0.5, 0.75, 1.0 } ),
                                                       m1,
                                                       MetricDimension.OBSERVED_VALUE,
                                                       MetricDimension.FORECAST_ERROR );
        //Check the results
        assertTrue( "The actual output for the box plot of forecast errors by observed value does not match the "
                    + "expected output.", actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link BoxPlotErrorByObserved#apply(EnsemblePairs)} when supplied with no data.
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
        BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( outF );
        b.setProbabilities( outF.vectorOf( new double[] { 0.0, 1.0 } ) );
        
        assertTrue( Objects.nonNull( b.build() ) );
    }
    
    /**
     * Tests for an expected exception on calling 
     * {@link BoxPlotErrorByObserved#apply(EnsemblePairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
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

        BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( outF );
        b.setProbabilities( outF.vectorOf( new double[] { 0.1 } ) );
        b.build();
    }
    
    /**
     * Tests for an expected exception on construction with negative probabilities. 
     */
    
    @Test
    public void testForExceptionOnNegativeProbabilities() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify only valid probabilities within [0,1] from which to construct the box plot." );

        BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( outF );
        b.setProbabilities( outF.vectorOf( new double[] { -0.1, 0.0, 0.5 } ) );
        b.build();
    }
    
    /**
     * Tests for an expected exception on construction with probabilities that are too high. 
     */
    
    @Test
    public void testForExceptionOnProbabilitiesGreaterThanOne() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify only valid probabilities within [0,1] from which to construct the box plot." );

        BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( outF );
        b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.5, 1.5 } ) );
        b.build();
    }    
    
    /**
     * Tests for an expected exception on construction with duplicate probabilities. 
     */
    
    @Test
    public void testForExceptionOnDuplicateProbabilities() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify only non-unique probabilities from which to construct the box plot." );

        BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( outF );
        b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.0, 1.0 } ) );
        b.build();
    }     
    
    /**
     * Tests for an expected exception on construction with an even number of probabilities. 
     */
    
    @Test
    public void testForExceptionOnEvenNumberOfProbabilities() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify an odd number of probabilities for the verification box plot." );

        BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( outF );
        b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.25, 0.5, 1.0 } ) );
        b.build();
    }  

    /**
     * Tests for an expected exception on construction with an even number of probabilities. 
     */
    
    @Test
    public void testForExceptionOnEvenNumberOf() throws MetricParameterException
    {
        BoxPlotErrorByObservedBuilder b = new BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder();
        b.setOutputFactory( outF );
        b.setProbabilities( outF.vectorOf( new double[] { 0.0, 0.25} ) );
        b.build();
    }  
    
}
