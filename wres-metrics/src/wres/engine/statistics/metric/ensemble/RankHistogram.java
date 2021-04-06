package wres.engine.statistics.metric.ensemble;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MissingValues;
import wres.datamodel.Slicer;
import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.engine.statistics.metric.Diagram;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

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
 */

public class RankHistogram extends Diagram<SampleData<Pair<Double, Ensemble>>, DiagramStatisticOuter>
{

    /**
     * Rank order.
     */

    public static final DiagramMetricComponent RANK_ORDER =
            DiagramMetricComponent.newBuilder()
                                  .setName( DiagramComponentName.RANK_ORDER )
                                  .setMinimum( 1 )
                                  .setMaximum( Double.POSITIVE_INFINITY )
                                  .setUnits( "COUNT" )
                                  .build();

    /**
     * Observed relative frequency.
     */

    public static final DiagramMetricComponent OBSERVED_RELATIVE_FREQUENCY = DiagramMetricComponent.newBuilder()
                                                                                                   .setName( DiagramComponentName.OBSERVED_RELATIVE_FREQUENCY )
                                                                                                   .setMinimum( 0 )
                                                                                                   .setMaximum( 1 )
                                                                                                   .setUnits( "PROBABILITY" )
                                                                                                   .build();

    /**
     * Basic description of the metric.
     */

    public static final DiagramMetric BASIC_METRIC = DiagramMetric.newBuilder()
                                                                  .setName( MetricName.RANK_HISTOGRAM )
                                                                  .build();

    /**
     * Full description of the metric.
     */

    public static final DiagramMetric METRIC = DiagramMetric.newBuilder()
                                                            .addComponents( RankHistogram.RANK_ORDER )
                                                            .addComponents( RankHistogram.OBSERVED_RELATIVE_FREQUENCY )
                                                            .setName( MetricName.RANK_HISTOGRAM )
                                                            .build();

    /**
     * A random number generator, used to assign ties randomly.
     */

    private Random rng;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static RankHistogram of()
    {
        return new RankHistogram();
    }

    /**
     * Returns an instance.
     * 
     * @param rng the random number generator for ties
     * @return an instance
     */

    public static RankHistogram of( Random rng )
    {
        return new RankHistogram( rng );
    }

    @Override
    public DiagramStatisticOuter apply( SampleData<Pair<Double, Ensemble>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        double[] ranks = new double[] { MissingValues.DOUBLE };
        double[] relativeFrequencies = new double[] { MissingValues.DOUBLE };

        // Some data to process
        if ( !s.getRawData().isEmpty() )
        {

            //Acquire subsets in case of missing data
            Map<Integer, List<Pair<Double, Ensemble>>> sliced =
                    Slicer.filterByRightSize( s.getRawData() );
            //Find the subset with the most elements
            Optional<List<Pair<Double, Ensemble>>> useMe =
                    sliced.values().stream().max( Comparator.comparingInt( List::size ) );

            if ( useMe.isPresent() )
            {
                //Set the ranked positions as 1:N+1
                ranks = IntStream.range( 1, useMe.get().get( 0 ).getRight().size() + 2 ).asDoubleStream().toArray();
                double[] sumRanks = new double[ranks.length]; //Total falling in each ranked position

                //Compute the sum of ranks
                BiConsumer<Pair<Double, Ensemble>, double[]> ranker = RankHistogram.rankWithTies( rng );
                useMe.get().forEach( nextPair -> ranker.accept( nextPair, sumRanks ) );

                //Compute relative frequencies
                relativeFrequencies = Arrays.stream( sumRanks ).map( a -> a / useMe.get().size() ).toArray();
            }
        }

        DiagramStatisticComponent ro =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RankHistogram.RANK_ORDER )
                                         .addAllValues( Arrays.stream( ranks )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatisticComponent obs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RankHistogram.OBSERVED_RELATIVE_FREQUENCY )
                                         .addAllValues( Arrays.stream( relativeFrequencies )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatistic histogram = DiagramStatistic.newBuilder()
                                                     .addStatistics( ro )
                                                     .addStatistics( obs )
                                                     .setMetric( RankHistogram.BASIC_METRIC )
                                                     .build();

        return DiagramStatisticOuter.of( histogram, s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.RANK_HISTOGRAM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    private RankHistogram()
    {
        super();
        this.rng = new Random();
    }

    /**
     * Hidden constructor.
     * 
     * @param rng the random number generator for ties
     */

    private RankHistogram( Random rng )
    {
        super();

        if ( Objects.nonNull( rng ) )
        {
            this.rng = rng;
        }
        else
        {
            this.rng = new Random();
        }
    }

    /**
     * Computes the zero-based rank position of the left-hand side of an ensemble pair within the right-hand side. 
     * When the right-hand side contains ties, the rank position is assigned randomnly. Increments the input array by 
     * one at the corresponding index.
     * 
     * @param rng the random number generator for handling ties
     * @return a function that increments the second argument based on the rank position of the observation within the ensemble
     */

    private static BiConsumer<Pair<Double, Ensemble>, double[]> rankWithTies( Random rng )
    {
        final TriConsumer<double[], Double, double[]> containedRanker = getContainedRanker( rng );
        return ( pair, sumRanks ) -> {
            //Sort the RHS
            double[] sorted = pair.getRight().getMembers();
            double obs = pair.getLeft();
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
