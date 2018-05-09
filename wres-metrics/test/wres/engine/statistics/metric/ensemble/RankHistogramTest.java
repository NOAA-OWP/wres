package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

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
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.ensemble.RankHistogram.RankHistogramBuilder;

/**
 * Tests the {@link RankHistogram}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class RankHistogramTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link RankHistogram}.
     */

    private RankHistogram rh;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;
    
    /**
     * Instance of a random number generator.
     */

    private Random rng;
    
    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        RankHistogramBuilder b = new RankHistogramBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        rng = new Random( 12345678 );
        b.setRNGForTies( rng );
        this.rh = b.build();
    }

    /**
     * Compares the output from {@link RankHistogram#apply(EnsemblePairs)} against expected output for pairs without
     * ties.
     */

    @Test
    public void testApplyWithoutTies() throws MetricParameterException
    {
        MetadataFactory metaFac = outF.getMetadataFactory();

        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        for ( int i = 0; i < 10000; i++ )
        {
            double left = rng.nextDouble();
            double[] right = new double[9];
            for ( int j = 0; j < 9; j++ )
            {
                right[j] = rng.nextDouble();
            }
            values.add( outF.pairOf( left, right ) );
        }

        final EnsemblePairs input = outF.ofEnsemblePairs( values, metaFac.getMetadata() );

        //Check the results       
        final MultiVectorOutput actual = rh.apply( input );
        double[] actualRanks = actual.get( MetricDimension.RANK_ORDER ).getDoubles();
        double[] actualRFreqs = actual.get( MetricDimension.OBSERVED_RELATIVE_FREQUENCY ).getDoubles();
        double[] expectedRanks = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double[] expectedRFreqs =
                new double[] { 0.0995, 0.1041, 0.0976, 0.1041, 0.0993, 0.1044, 0.1014, 0.0952, 0.0972, 0.0972 };

        //Check the first pair of quantiles, which should map to the first entry, since the lower bound is unknown
        assertTrue( "Difference between actual and expected rank positions.",
                    Arrays.equals( actualRanks, expectedRanks ) );
        assertTrue( "Difference between actual and expected relative frequencies.",
                    Arrays.equals( actualRFreqs, expectedRFreqs ) );
    }
    
    /**
     * Compares the output from {@link RankHistogram#apply(EnsemblePairs)} against expected output for pairs with
     * ties.
     */

    @Test
    public void testApplyWithTies()
    {
        MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data using an RNG for a uniform U[0,1] distribution with a fixed seed
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( outF.pairOf( 2, new double[] { 1, 2, 2, 2, 4, 5, 6, 7, 8 } ) );
        final EnsemblePairs input = outF.ofEnsemblePairs( values, metaFac.getMetadata() );

        //Check the results       
        final MultiVectorOutput actual = rh.apply( input );

        double[] actualRanks = actual.get( MetricDimension.RANK_ORDER ).getDoubles();
        double[] actualRFreqs = actual.get( MetricDimension.OBSERVED_RELATIVE_FREQUENCY ).getDoubles();
        double[] expectedRanks = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double[] expectedRFreqs =
                new double[] { 0, 0, 0, 1, 0, 0, 0, 0, 0, 0 };

        //Check the first pair of quantiles, which should map to the first entry, since the lower bound is unknown
        assertTrue( "Difference between actual and expected rank positions.",
                    Arrays.equals( actualRanks, expectedRanks ) );
        assertTrue( "Difference between actual and expected relative frequencies.",
                    Arrays.equals( actualRFreqs, expectedRFreqs ) );
    }
    

    /**
     * Validates the output from {@link RankHistogram#apply(EnsemblePairs)} when 
     * supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        EnsemblePairs input =
                outF.ofEnsemblePairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );

        MultiVectorOutput actual = rh.apply( input );
        
        double[] source = new double[1];
        
        Arrays.fill( source, Double.NaN );

        assertTrue( Arrays.equals( actual.getData()
                          .get( MetricDimension.RANK_ORDER )
                          .getDoubles(), source ) );

        assertTrue( Arrays.equals( actual.getData()
                          .get( MetricDimension.OBSERVED_RELATIVE_FREQUENCY )
                          .getDoubles(), source ) );
    }

    /**
     * Checks that the {@link RankHistogram#getName()} returns 
     * {@link MetricConstants.RANK_HISTOGRAM.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( rh.getName().equals( MetricConstants.RANK_HISTOGRAM.toString() ) );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link RankHistogram#apply(EnsemblePairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'RANK HISTOGRAM'." );
        
        rh.apply( null );
    }    

    
    /**
     * Tests for the correct construction of a {@link RankHistogram} when a random number generator is not supplied.
     * @throws MetricParameterException if construction fails for an unexpected reason
     */

    @Test
    public void testConstructionWithoutRNG() throws MetricParameterException
    {
        RankHistogramBuilder b = new RankHistogramBuilder();
        b.setOutputFactory( outF );
        assertTrue( Objects.nonNull( b.build() ) );               
    }  
    
}
