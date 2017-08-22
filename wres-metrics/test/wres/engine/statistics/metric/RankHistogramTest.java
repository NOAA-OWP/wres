package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.engine.statistics.metric.RankHistogram.RankHistogramBuilder;

/**
 * Tests the {@link RankHistogram}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class RankHistogramTest
{

    /**
     * Constructs a {@link RankHistogram} and compares the actual result to the expected result for a synthetic dataset,
     * involving sampling from a uniform probability distribution whose expected frequencies are 1/N+1, where N=9 
     * ensemble members. Also, checks the parameters of the metric.
     */

    @Test
    public void test1RankHistogram()
    {
        //Build the metric
        final RankHistogramBuilder b = new RankHistogramBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory( outF );

        final RankHistogram rh = b.build();

        //Generate some data using an RNG for a uniform U[0,1] distribution with a fixed seed
        Random r = new Random( 12345678 );
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        for ( int i = 0; i < 10000; i++ )
        {
            double left = r.nextDouble();
            double[] right = new double[9];
            for ( int j = 0; j < 9; j++ )
            {
                right[j] = r.nextDouble();
            }
            values.add( outF.pairOf( left, right ) );
        }

        final EnsemblePairs input = outF.ofEnsemblePairs( values, metaFac.getMetadata() );

        //Check the results       
        final MultiVectorOutput actual = rh.apply( input );
        double[] actualRanks = actual.get( MetricConstants.RANK_ORDER ).getDoubles();
        double[] actualRFreqs = actual.get( MetricConstants.OBSERVED_RELATIVE_FREQUENCY ).getDoubles();
        double[] expectedRanks = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double[] expectedRFreqs =
                new double[] { 0.0995, 0.1041, 0.0976, 0.1041, 0.0993, 0.1044, 0.1014, 0.0952, 0.0972, 0.0972 };

        //Check the first pair of quantiles, which should map to the first entry, since the lower bound is unknown
        assertTrue( "Difference between actual and expected rank positions.",
                    Arrays.equals( actualRanks, expectedRanks ) );
        assertTrue( "Difference between actual and expected relative frequencies.",
                    Arrays.equals( actualRFreqs, expectedRFreqs ) );
        
        //Check the parameters
        assertTrue( "Unexpected name for the Rank Histogram.",
                    rh.getName().equals( metaFac.getMetricName( MetricConstants.RANK_HISTOGRAM ) ) );

    }

}
