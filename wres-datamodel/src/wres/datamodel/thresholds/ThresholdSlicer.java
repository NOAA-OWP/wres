package wres.datamodel.thresholds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;

/**
 * Class for slicing and dicing thresholds.
 * 
 * @author James Brown
 */

public class ThresholdSlicer
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdSlicer.class );

    /**
     * The number of decimal places to use when rounding.
     */

    private static final int DECIMALS = 5;

    /**
     * Compares thresholds for logical similarity. Logical similarity is weaker than content equivalence or identity 
     * equivalence and considers only some attributes of the threshold, where appropriate. A logical comparison looks 
     * at:
     * 
     * <ol>
     * <li>The threshold names for thresholds that are named, i.e., {@link ThresholdOuter#hasLabel()} returns 
     * {@code true} and {@link ThresholdOuter#getLabel()} match (e.g., "flood"), else;</li>
     * <li>The threshold probabilities for probability thresholds, else;</li>
     * <li>All of the threshold components.</li>
     * </ol>
     * 
     * @return a threshold comparator that examines the logical similarity of two thresholds
     */

    public static Comparator<OneOrTwoThresholds> getLogicalThresholdComparator()
    {
        // First, create a comparator that compares two threshold tuples.
        return ( OneOrTwoThresholds one, OneOrTwoThresholds another ) -> {

            // Compare the second/decision threshold first, which is compared on all content if it exists.
            int compare = Objects.compare( one.second(),
                                           another.second(),
                                           Comparator.nullsFirst( Comparator.naturalOrder() ) );

            if ( compare != 0 )
            {
                return compare;
            }

            // Compare the first threshold by label if both have a label. Thresholds with a label have the same meaning
            // across features.
            if ( one.first().hasLabel() && another.first().hasLabel() )
            {
                return Objects.compare( one.first().getLabel(),
                                        another.first().getLabel(),
                                        Comparator.naturalOrder() );
            }

            // Compare by probability threshold if both are probability thresholds. Thresholds that are probability 
            // thresholds have the same meaning across features.
            if ( one.first().hasProbabilities() && another.first().hasProbabilities() )
            {
                return Objects.compare( one.first().getProbabilities(),
                                        another.first().getProbabilities(),
                                        Comparator.naturalOrder() );
            }

            // Resort to a full comparison.           
            return Objects.compare( one,
                                    another,
                                    Comparator.nullsFirst( Comparator.naturalOrder() ) );
        };
    }

    /**
     * <p>Decomposes a map of thresholds into N collections, each of which contains one logical threshold for each of 
     * the available keys. Thresholds are logically equivalent if they are equal according to 
     * {@link #getLogicalThresholdComparator()}.
     * 
     * @param <T> the type of key
     * @param thresholds the thresholds to decompose
     * @return the decomposed thresholds, one (map) for each logical threshold across all keys
     */

    public static <T> List<Map<T, ThresholdOuter>> decompose( Map<T, Set<ThresholdOuter>> thresholds )
    {
        Objects.requireNonNull( thresholds );

        // Decompose into logical thresholds. There will be as many maps returned as logical thresholds
        Comparator<ThresholdOuter> comparator =
                ( one, two ) -> ThresholdSlicer.getLogicalThresholdComparator()
                                               .compare( OneOrTwoThresholds.of( one ), OneOrTwoThresholds.of( two ) );
        Set<ThresholdOuter> logicalThresholds = new TreeSet<>( comparator );
        thresholds.values()
                  .stream()
                  .flatMap( Set::stream )
                  .forEach( logicalThresholds::add );

        List<Map<T, ThresholdOuter>> returnMe = new ArrayList<>();

        for ( ThresholdOuter nextThreshold : logicalThresholds )
        {
            Map<T, ThresholdOuter> nextMap = new HashMap<>();

            for ( Map.Entry<T, Set<ThresholdOuter>> nextEntry : thresholds.entrySet() )
            {
                T nextKey = nextEntry.getKey();
                Set<ThresholdOuter> nextThresholds = nextEntry.getValue();
                Optional<ThresholdOuter> aMatchingThreshold =
                        nextThresholds.stream()
                                      .filter( next -> comparator.compare( next, nextThreshold ) == 0 )
                                      .findAny();

                // Add to the map
                if ( aMatchingThreshold.isPresent() )
                {
                    nextMap.put( nextKey, aMatchingThreshold.get() );
                }
            }

            // Add to the list
            if ( !nextMap.isEmpty() )
            {
                returnMe.add( Collections.unmodifiableMap( nextMap ) );
            }
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * <p>Unpacks a map of {@link ThresholdsByMetric} into a collection organized by feature alone, forming the union 
     * of thresholds across metrics.
     * 
     * @param <T> the type of key
     * @param thresholds the thresholds to decompose
     * @return the decomposed thresholds
     */

    public static <T> Map<T, Set<ThresholdOuter>> unpack( Map<T, ThresholdsByMetric> thresholds )
    {
        Objects.requireNonNull( thresholds );

        return thresholds.entrySet()
                         .stream()
                         .map( next -> Map.entry( next.getKey(), next.getValue().union() ) )
                         .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    /**
     * Applies {@link ThresholdsByMetric#filterByGroup(SampleDataGroup, StatisticType)}, followed by 
     * {@link ThresholdsByMetric#filterByGroup(ThresholdGroup...)} to each element in the input and returns the result.
     * 
     * @param <T> the type of key
     * @param thresholds the thresholds
     * @param sampleDataGroup the sample data group filter
     * @param statisticType the statistic type filter
     * @param thresholdGroups the threshold group filters
     * @return the filtered thresholds
     * @throws NullPointerException if any input is null
     */

    public static <T> Map<T, ThresholdsByMetric> filterByGroup( Map<T, ThresholdsByMetric> thresholds,
                                                                SampleDataGroup sampleDataGroup,
                                                                StatisticType statisticType,
                                                                ThresholdGroup... thresholdGroups )
    {
        Objects.requireNonNull( thresholds );
        Objects.requireNonNull( sampleDataGroup );
        Objects.requireNonNull( statisticType );
        Objects.requireNonNull( thresholdGroups );

        Map<T, ThresholdsByMetric> returnMe = new HashMap<>();

        for ( Map.Entry<T, ThresholdsByMetric> nextEntry : thresholds.entrySet() )
        {
            T nextKey = nextEntry.getKey();
            ThresholdsByMetric nextThresholds = nextEntry.getValue();

            if ( Objects.nonNull( nextThresholds ) )
            {
                ThresholdsByMetric filtered =
                        nextThresholds.filterByGroup( sampleDataGroup, statisticType ).filterByGroup( thresholdGroups );
                returnMe.put( nextKey, filtered );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "When filtering thresholds by group, the thresholds associated with key {} "
                              + "were null and will not be included in the result.",
                              nextKey );
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Adds a quantile to each probability threshold in the input using the climatological data provided by the pool 
     * and a mapping function that maps from the pool metadata to the keyed data.
     * 
     * @param <S> the type of key
     * @param <T> the type of pool data
     * @param thresholds the thresholds
     * @param pool the pool
     * @param metaMapper the function that maps from the pool metadata to the map key
     * @return the thresholds with quantiles added
     */

    public static <S extends Comparable<S>, T> Map<S, Set<ThresholdOuter>>
            addQuantiles( Map<S, Set<ThresholdOuter>> thresholds,
                          Pool<T> pool,
                          Function<PoolMetadata, S> metaMapper )
    {
        Objects.requireNonNull( thresholds );
        Objects.requireNonNull( pool );
        Objects.requireNonNull( metaMapper );

        Map<S, Pool<T>> decomposed = PoolSlicer.decompose( metaMapper, pool );

        Map<S, Set<ThresholdOuter>> returnMe = new HashMap<>();
        for ( Map.Entry<S, Pool<T>> nextEntry : decomposed.entrySet() )
        {
            S nextKey = nextEntry.getKey();
            Pool<T> nextPool = nextEntry.getValue();
            Set<ThresholdOuter> nextThresholdSet = thresholds.get( nextKey );

            // Probability threshold available
            boolean hasProbabilityThreshold = Objects.nonNull( nextThresholdSet )
                                              && nextThresholdSet.stream()
                                                                 .anyMatch( ThresholdOuter::hasProbabilities );
            if ( nextPool.hasClimatology() && hasProbabilityThreshold )
            {
                VectorOfDoubles climatology = nextPool.getClimatology();
                double[] sorted = climatology.getDoubles();
                Arrays.sort( sorted );

                // Quantile mapper
                UnaryOperator<ThresholdOuter> quantileMapper =
                        probThreshold -> Slicer.getQuantileFromProbability( probThreshold,
                                                                            sorted,
                                                                            ThresholdSlicer.DECIMALS );
                // Quantiles
                Set<ThresholdOuter> quantiles = nextThresholdSet.stream()
                                                                .filter( ThresholdOuter::hasProbabilities )
                                                                .map( quantileMapper )
                                                                .collect( Collectors.toUnmodifiableSet() );

                returnMe.put( nextKey, quantiles );
            }
            else
            {
                LOGGER.debug( "Failed to add quantile thresholds for {}. In order to add a quantile threshold, a "
                              + "probability threshold and some climatological data must both be available. Climatological "
                              + "data: {}. Probability threshold found: {}.",
                              nextKey,
                              nextPool.hasClimatology(),
                              hasProbabilityThreshold );
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Generates filters from thresholds for each threshold in the mapped input using a filter generator.
     * @param <S> the key
     * @param <T> the type of data to filter
     * @param thresholds the thresholds
     * @param filterGenerator the function that generates a filter
     * @return the filters
     */

    public static <S, T> Map<S, Predicate<T>> getFiltersFromThresholds( Map<S, ThresholdOuter> thresholds,
                                                                        Function<ThresholdOuter, Predicate<T>> filterGenerator )
    {
        Objects.requireNonNull( thresholds );
        Objects.requireNonNull( filterGenerator );

        return thresholds.entrySet()
                         .stream()
                         .map( next -> Map.entry( next.getKey(), filterGenerator.apply( next.getValue() ) ) )
                         .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    /**
     * Do not construct.
     */

    private ThresholdSlicer()
    {
    }

}
