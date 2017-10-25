package wres.engine.statistics.metric;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import wres.datamodel.DataFactory;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.Slicer;

/**
 * <p>Computes the probability (as a relative fraction) that the observation falls between any two ranked ensemble 
 * members. Also known as the Talagrand diagram, and analogous to the Probability Integral Transform (PIT) for 
 * probabilistic predictions.</p>
 *  
 * <p>Ties are assigned randomly. As such, the output from the {@link RankHistogram} is non-deterministic when one or 
 * more pairs contains ensemble members with identical values. When one or more pairs contain missing values, the
 * {@link RankHistogram} is computed from the largest subset of pairs with an equal number of (non-missing) ensemble 
 * members.</p> 
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class RankHistogram extends Metric<EnsemblePairs, MultiVectorOutput>
{

    /**
     * A random number generator, used to assign ties randomly.
     */

    private Random rng;

    @Override
    public MultiVectorOutput apply( EnsemblePairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        DataFactory d = getDataFactory();
        Slicer slicer = d.getSlicer();
        //Acquire subsets in case of missing data
        Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> sliced = slicer.sliceByRight( s.getData() );
        //Find the subset with the most elements
        List<PairOfDoubleAndVectorOfDoubles> useMe =
                sliced.values().stream().max( Comparator.comparingInt( List::size ) ).get();

        //Set the ranked positions as 1:N+1
        double[] ranks = IntStream.range( 1, useMe.get( 0 ).getItemTwo().length + 2 ).asDoubleStream().toArray();
        double[] sumRanks = new double[ranks.length]; //Total falling in each ranked position

        //Compute the sum of ranks
        BiConsumer<PairOfDoubleAndVectorOfDoubles, double[]> ranker = rankWithTies( rng );
        useMe.forEach( nextPair -> ranker.accept( nextPair, sumRanks ) );

        //Compute relative frequencies
        double[] relativeFrequencies = Arrays.stream( sumRanks ).map( a -> a / useMe.size() ).toArray();

        //Set and return the results
        Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
        output.put( MetricDimension.RANK_ORDER, ranks );
        output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, relativeFrequencies );
        final MetricOutputMetadata metOut = getMetadata( s, s.getData().size(), MetricConstants.MAIN, null );
        return d.ofMultiVectorOutput( output, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.RANK_HISTOGRAM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class RankHistogramBuilder extends MetricBuilder<EnsemblePairs, MultiVectorOutput>
    {
        /**
         * Random number generator to assign ties randomly.
         */

        private Random rng;

        @Override
        protected RankHistogram build() throws MetricParameterException
        {
            return new RankHistogram( this );
        }

        /**
         * Optionally, assign a random number generator for resolving ties.
         * 
         * @param rng the random number generator
         */
        RankHistogramBuilder setRNGForTies( Random rng )
        {
            this.rng = rng;
            return this;
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private RankHistogram( final RankHistogramBuilder builder ) throws MetricParameterException
    {
        super( builder );
        if ( Objects.nonNull( builder.rng ) )
        {
            rng = builder.rng;
        }
        else
        {
            rng = new Random();
        }
    }

    /**
     * Computes the zero-based rank position of the left-hand side of the {@link PairOfDoubleAndVectorOfDoubles} within
     * the right-hand side. When the right-hand side contains ties, the rank position is assigned randomnly. Increments 
     * the input array by one at the corresponding index.
     * 
     * @param rng the random number generator for handling ties
     * @return a function that increments the second argument based on the rank position of the observation within the ensemble
     */

    private static BiConsumer<PairOfDoubleAndVectorOfDoubles, double[]> rankWithTies( Random rng )
    {
        final TriConsumer<double[], Double, double[]> containedRanker = getContainedRanker( rng );
        return ( pair, sumRanks ) -> {
            //Sort the RHS
            double[] sorted = pair.getItemTwo();
            double obs = pair.getItemOne();
            Arrays.sort( sorted );
            //Miss low
            if ( obs < sorted[0] )
            {
                sumRanks[0] += 1;
            }
            //Greater or equal to upper bound
            else if ( obs >= sorted[sorted.length - 1] )
            {
                sumRanks[sumRanks.length - 1] += 1;
            }
            //Contained
            else
            {
                containedRanker.accept( sorted, obs, sumRanks );
            }
        };
    }

    /**
     * Increments the ranked position within the input array when the observation is contained by the ensemble.
     * 
     * @param rng the random number generator for handling ties
     * @return a function that increments the ranked position with the observation is contained
     */

    private static TriConsumer<double[], Double, double[]> getContainedRanker( Random rng )
    {
        final TriConsumer<double[], Integer, double[]> tiedRanker = getTiedRanker( rng );
        return ( sorted, obs, sumRanks ) -> {
            for ( int k = 0; k < sorted.length; k++ )
            {
                //Bin located
                if ( obs <= sorted[k] )
                {
                    //Unique
                    if ( k < ( sorted.length - 1 ) && sorted[k] < sorted[k + 1] )
                    {
                        sumRanks[k] += 1;
                    }
                    //Tied, find the random rank
                    else
                    {
                        tiedRanker.accept( sorted, k, sumRanks );
                    }
                    break;
                }
            }
        };
    }

    /**
     * Increments the ranked position within the input array when the observation is contained by the ensemble and
     * the ensemble includes tied ranks.
     * 
     * @param rng the random number generator for handling ties
     * @return a function that increments the ranked position when the observation is contained and the ensemble 
     *            includes ties
     */

    private static TriConsumer<double[], Integer, double[]> getTiedRanker( Random rng )
    {
        return ( sorted, lowerBound, sumRanks ) -> {
            int startRank = lowerBound; //Lower bound of tie
            int endRank = lowerBound; //Upper bound of tie, TBD
            //Locate upper bound
            for ( int j = lowerBound + 1; j < sorted.length; j++ )
            {
                if ( Math.abs( sorted[lowerBound] - sorted[j] ) < .0000001 )
                {
                    endRank += 1;
                }
                else
                {
                    break;
                }
            }
            //Same lower and upper bound
            if ( startRank == endRank )
            {
                sumRanks[startRank] += 1;
            }
            //Select a random rank between upper and lower
            else
            {
                int adj = endRank - startRank;
                sumRanks[rng.nextInt( adj + 1 ) + startRank] += 1;
            }
        };
    }

    @FunctionalInterface
    private interface TriConsumer<T, U, S>
    {
        /**
        * Consume three inputs.
        *
        * @param t the first input
        * @param u the second input
        * @param s the third input
        */
        void accept( T t, U u, S s );
    }

}
