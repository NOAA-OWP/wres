package wres.datamodel.thresholds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.types.Climatology;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.units.UnitMapper;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Threshold;

/**
 * Class for slicing and dicing thresholds.
 *
 * @author James Brown
 */

public class ThresholdSlicer
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdSlicer.class );

    /** The number of decimal places to use when rounding. */
    private static final int DECIMALS = 5;

    /**
     * Filters thresholds that are equal except for their probabilities. The purpose of this method is to de-duplicate 
     * thresholds that were generated from climatological probabilities and whose quantiles are unknown at declaration 
     * time. For example, with precipitation and other mixed probability distributions, many probabilities will map to 
     * the same (zero) quantiles and including these duplicates is wasteful. Among the equal thresholds, retains the
     * one with the largest probability value.
     *
     * @param thresholds the thresholds to de-duplicate
     * @return the thresholds whose real values and/or names are different
     */

    public static Set<ThresholdOuter> filter( Set<ThresholdOuter> thresholds )
    {
        Objects.requireNonNull( thresholds );

        // Remove the probabilities from all thresholds and then group the original thresholds by these new thresholds
        // Finally, pick the largest threshold from each group      
        Map<ThresholdOuter, SortedSet<ThresholdOuter>> mappedThresholds = new TreeMap<>();

        for ( ThresholdOuter next : thresholds )
        {
            // Has value thresholds?
            if ( next.hasValues() )
            {
                Threshold.Builder builder = next.getThreshold()
                                                .toBuilder();
                builder.clearLeftThresholdProbability();
                builder.clearRightThresholdProbability();

                ThresholdOuter noProbs =
                        new ThresholdOuter.Builder( builder.build() ).build();

                if ( mappedThresholds.containsKey( noProbs ) )
                {
                    mappedThresholds.get( noProbs )
                                    .add( next );
                }
                else
                {
                    SortedSet<ThresholdOuter> nextGroup = new TreeSet<>();
                    nextGroup.add( next );
                    mappedThresholds.put( noProbs, nextGroup );
                }
            }
            // Only probabilities, so allow without filtering
            else
            {
                SortedSet<ThresholdOuter> nextGroup = new TreeSet<>();
                nextGroup.add( next );
                mappedThresholds.put( next, nextGroup );
            }
        }

        return mappedThresholds.values()
                               .stream()
                               .map( SortedSet::last )
                               .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Applies a filter function to each set of thresholds in the input.
     * @param <S> the key type
     * @param thresholds the thresholds, not null
     * @param filter the filter, not null
     * @return the filtered thresholds
     */

    public static <S> Map<S, Set<ThresholdOuter>> filter( Map<S, Set<ThresholdOuter>> thresholds,
                                                          UnaryOperator<Set<ThresholdOuter>> filter )
    {
        Objects.requireNonNull( thresholds );
        Objects.requireNonNull( filter );

        return thresholds.entrySet()
                         .stream()
                         .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                                 nextEntry -> filter.apply( nextEntry.getValue() ) ) );
    }

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
            if ( one.first()
                    .hasLabel()
                 && another.first()
                           .hasLabel() )
            {
                return Objects.compare( one.first()
                                           .getLabel(),
                                        another.first()
                                               .getLabel(),
                                        Comparator.naturalOrder() );
            }

            // Compare by probability threshold if both are probability thresholds. Thresholds that are probability 
            // thresholds have the same meaning across features.
            if ( one.first()
                    .hasProbabilities()
                 && another.first()
                           .hasProbabilities() )
            {
                return Objects.compare( one.first()
                                           .getProbabilities(),
                                        another.first()
                                               .getProbabilities(),
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
     * {@link #getLogicalThresholdComparator()}. Allows for duplicates: see #117102.
     *
     * @param <T> the type of key
     * @param thresholds the thresholds to decompose
     * @return the decomposed thresholds, one (map) for each logical threshold across all keys
     */

    public static <T> List<Map<T, ThresholdOuter>> decompose( Map<T, Set<ThresholdOuter>> thresholds )
    {
        Objects.requireNonNull( thresholds );

        // Decompose into logical thresholds. There will be as many maps returned as logical thresholds where each
        // duplicate counts as a separate logical threshold. For example, if there are two thresholds named "banana"
        // for the same key of T=t and the logical comparator looks at threshold name only, then there will be two
        // separate "banana" thresholds at T=t.
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
            // As many maps as duplicate logical thresholds
            List<Map<T, ThresholdOuter>> nextMaps = new ArrayList<>();
            for ( Map.Entry<T, Set<ThresholdOuter>> nextEntry : thresholds.entrySet() )
            {
                T nextKey = nextEntry.getKey();
                Set<ThresholdOuter> nextThresholds = nextEntry.getValue();
                // Sorted set of matching thresholds: insofar as the duplicates are ordered, this will maintain the
                // relative positions of the duplicates
                Set<ThresholdOuter> matchingThresholds =
                        nextThresholds.stream()
                                      .filter( next -> comparator.compare( next, nextThreshold ) == 0 )
                                      .collect( Collectors.toCollection( TreeSet::new ) );

                int count = 0;
                for ( ThresholdOuter nextDuplicate : matchingThresholds )
                {
                    if ( count >= nextMaps.size() )
                    {
                        Map<T, ThresholdOuter> nextMap = new HashMap<>();
                        nextMaps.add( nextMap );
                    }

                    Map<T, ThresholdOuter> nextMap = nextMaps.get( count );
                    nextMap.put( nextKey, nextDuplicate );
                    count++;
                }
            }

            // Add to the list
            returnMe.addAll( nextMaps );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Finds the common attributes among the input thresholds and returns a threshold that contains them.
     *
     * @param thresholds the input thresholds
     * @return the composed threshold
     * @throws NullPointerException if the thresholds is null or any threshold is null
     * @throws ThresholdException if there are no thresholds or the thresholds cannot be validly composed
     */

    public static ThresholdOuter compose( Set<ThresholdOuter> thresholds )
    {
        Objects.requireNonNull( thresholds );

        if ( thresholds.isEmpty() )
        {
            throw new ThresholdException( "Cannot compose an empty set of thresholds." );
        }

        if ( thresholds.size() == 1 )
        {
            return thresholds.iterator().next();
        }

        // Set the common attributes
        Set<OneOrTwoDoubles> values = new HashSet<>();
        Set<OneOrTwoDoubles> probabilities = new HashSet<>();
        Set<String> names = new HashSet<>();
        Set<ThresholdOperator> operators = new HashSet<>();
        Set<ThresholdOrientation> thresholdDataTypes = new HashSet<>();
        Set<MeasurementUnit> measurementUnits = new HashSet<>();

        for ( ThresholdOuter next : thresholds )
        {
            names.add( next.getLabel() );
            probabilities.add( next.getProbabilities() );
            values.add( next.getValues() );
            measurementUnits.add( next.getUnits() );
            operators.add( next.getOperator() );
            thresholdDataTypes.add( next.getOrientation() );
        }

        // Remove any nullable attributes that had nulls
        names.remove( null );
        probabilities.remove( null );
        values.remove( null );
        measurementUnits.remove( null );

        ThresholdOuter.Builder builder = new ThresholdOuter.Builder();

        // Operator and data type are required
        if ( operators.size() == 1 )
        {
            builder.setOperator( operators.iterator()
                                          .next() );
        }
        else
        {
            throw new ThresholdException( "Cannot compose thresholds without a common operator. Discovered "
                                          + "these operators: "
                                          + operators
                                          + "." );
        }

        if ( thresholdDataTypes.size() == 1 )
        {
            builder.setOrientation( thresholdDataTypes.iterator().next() );
        }
        else
        {
            throw new ThresholdException( "Cannot compose thresholds without a common data type. Discovered "
                                          + "these data types: "
                                          + thresholdDataTypes
                                          + "." );
        }

        // Set the values and probabilities depending on what is available
        ThresholdSlicer.setValuesAndProbabilities( builder, values, probabilities );

        if ( names.size() == 1 )
        {
            builder.setLabel( names.iterator()
                                   .next() );
        }

        if ( measurementUnits.size() == 1 )
        {
            builder.setUnits( measurementUnits.iterator()
                                              .next() );
        }

        return builder.build();
    }

    /**
     * Adds a quantile to each probability threshold in the input using the climatological data. Preserves any 
     * thresholds that do not require quantiles. Uses the left-ish feature to correlate with the climatological data.
     *
     * @param thresholds the thresholds
     * @param climatology the climatology
     * @return the thresholds with quantiles added
     * @throws ThresholdException if the climatology is required but null
     */

    public static Map<FeatureTuple, Set<ThresholdOuter>> addQuantiles( Map<FeatureTuple, Set<ThresholdOuter>> thresholds,
                                                                       Climatology climatology )
    {
        Objects.requireNonNull( thresholds );

        // No probability thresholds, return the input thresholds
        if ( thresholds.values()
                       .stream()
                       .flatMap( Set::stream )
                       .noneMatch( ThresholdOuter::hasProbabilities ) )
        {
            LOGGER.debug( "No probability thresholds discovered, returning the input thresholds without quantiles." );

            return thresholds;
        }

        Map<FeatureTuple, Set<ThresholdOuter>> returnMe = new HashMap<>();
        for ( Map.Entry<FeatureTuple, Set<ThresholdOuter>> nextThresholds : thresholds.entrySet() )
        {
            FeatureTuple nextFeature = nextThresholds.getKey();
            Set<ThresholdOuter> nextThresholdSet = nextThresholds.getValue();

            // Probability threshold available for this pool
            boolean hasProbabilityThreshold = Objects.nonNull( nextThresholdSet )
                                              && nextThresholdSet.stream()
                                                                 .anyMatch( ThresholdOuter::hasProbabilities );
            if ( hasProbabilityThreshold )
            {
                Feature leftFeature = nextFeature.getLeft();

                if ( Objects.isNull( climatology )
                     || climatology.hasNoClimatology( leftFeature ) )
                {
                    throw new ThresholdException( "Quantiles were required for feature tuple '"
                                                  + nextFeature.toStringShort()
                                                  + "' but no climatological data was found for the left feature, '"
                                                  + leftFeature.getName()
                                                  + "'." );
                }

                double[] sorted = climatology.get( leftFeature );

                // Quantile mapper
                UnaryOperator<ThresholdOuter> quantileMapper =
                        probThreshold ->
                        {
                            // Add the unit for the quantile
                            wres.statistics.generated.Threshold probThresholdWithUnit =
                                    probThreshold.getThreshold()
                                                 .toBuilder()
                                                 .setThresholdValueUnits( climatology.getMeasurementUnit() )
                                                 .build();
                            ThresholdOuter threshold = ThresholdOuter.of( probThresholdWithUnit );
                            return Slicer.getQuantileFromProbability( threshold,
                                                                      sorted,
                                                                      ThresholdSlicer.DECIMALS );
                        };
                // Quantiles
                Set<ThresholdOuter> quantiles = nextThresholdSet.stream()
                                                                .filter( ThresholdOuter::hasProbabilities )
                                                                .map( quantileMapper )
                                                                .collect( Collectors.toCollection( HashSet::new ) );

                // Thresholds without probabilities
                nextThresholdSet.stream()
                                .filter( Predicate.not( ThresholdOuter::hasProbabilities ) )
                                .forEach( quantiles::add );

                returnMe.put( nextFeature, quantiles );
            }
            else if ( Objects.nonNull( nextThresholdSet ) )
            {
                LOGGER.debug( "Failed to add quantile thresholds for {}. In order to add a quantile threshold, a "
                              + "probability threshold and some climatological data must both be available. "
                              + "Climatological data found: {}. Probability threshold found: false.",
                              nextFeature,
                              Objects.nonNull( climatology ) );

                // Preserve them
                returnMe.put( nextFeature, nextThresholdSet );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While attempting to add quantiles to thresholds, failed to find a correlation "
                              + "in the threshold map for key {}. The available keys were {}.",
                              nextFeature,
                              thresholds.keySet() );
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
     * Generates transformers from thresholds for each threshold in the mapped input using a transformer generator.
     * @param <S> the key
     * @param <T> the type of data to transform
     * @param <U> the transformed data type
     * @param thresholds the thresholds
     * @param transformerGenerator the function that generates a filter
     * @return the filters
     */

    public static <S, T, U> Map<S, Function<T, U>> getTransformersFromThresholds( Map<S, ThresholdOuter> thresholds,
                                                                                  Function<ThresholdOuter, Function<T, U>> transformerGenerator )
    {
        Objects.requireNonNull( thresholds );
        Objects.requireNonNull( transformerGenerator );

        return thresholds.entrySet()
                         .stream()
                         .map( next -> Map.entry( next.getKey(), transformerGenerator.apply( next.getValue() ) ) )
                         .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    /**
     * <p>Returns the composed thresholds associated with each metric in the input container. A composed threshold is a 
     * {@link OneOrTwoThresholds} that contains two thresholds if the metric consumes 
     * {@link SampleDataGroup#DICHOTOMOUS} and has {@link ThresholdType#PROBABILITY_CLASSIFIER},
     * otherwise one threshold. The thresholds are stored in natural order.
     *
     * @param metrics the metrics
     * @param thresholds the thresholds
     * @return the composed thresholds
     * @throws NullPointerException if the thresholds is null
     */

    public static Map<MetricConstants, SortedSet<OneOrTwoThresholds>> getOneOrTwoThresholds( Set<MetricConstants> metrics,
                                                                                             Set<ThresholdOuter> thresholds )
    {
        Objects.requireNonNull( metrics );
        Objects.requireNonNull( thresholds );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> returnMe = new EnumMap<>( MetricConstants.class );

        // Iterate the metrics
        for ( MetricConstants next : metrics )
        {
            //Non-classifiers
            Set<ThresholdOuter> eventThresholds = getBasicEventThresholdsForMetric( next, thresholds );

            // Thresholds to add
            SortedSet<OneOrTwoThresholds> oneOrTwo = new TreeSet<>();

            // Dichotomous metrics with classifiers
            if ( next.isInGroup( SampleDataGroup.DICHOTOMOUS )
                 && eventThresholds.size() < thresholds.size() )
            {
                // Classifiers
                Set<ThresholdOuter> classifiers = thresholds.stream()
                                                            .filter( n -> n.getType()
                                                                          == ThresholdType.PROBABILITY_CLASSIFIER )
                                                            .collect( Collectors.toSet() );

                // One composed threshold for each event threshold and combination of event and decision threshold,
                // as dichotomous metrics can be computed for both
                for ( ThresholdOuter first : eventThresholds )
                {
                    OneOrTwoThresholds nextOuter = OneOrTwoThresholds.of( first );
                    oneOrTwo.add( nextOuter );
                    for ( ThresholdOuter second : classifiers )
                    {
                        OneOrTwoThresholds nextInner = OneOrTwoThresholds.of( first, second );
                        oneOrTwo.add( nextInner );
                    }
                }
            }
            // All other metrics
            else
            {
                for ( ThresholdOuter first : eventThresholds )
                {
                    oneOrTwo.add( OneOrTwoThresholds.of( first ) );
                }
            }

            // Update container
            SortedSet<OneOrTwoThresholds> nextSet = Collections.unmodifiableSortedSet( oneOrTwo );
            returnMe.put( next, nextSet );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Creates one {@link MetricsAndThresholds} for each group of {@link Metric} in the supplied declaration that can
     * be processed together as an atomic unit (i.e., they all consume the same pool of pairs).
     *
     * @param evaluation the evaluation declaration
     * @return one {@link MetricsAndThresholds} for each atomic group of metrics
     * @throws IllegalStateException if there are no features or metrics
     */

    public static Set<MetricsAndThresholds> getMetricsAndThresholdsForProcessing( EvaluationDeclaration evaluation )
    {
        Objects.requireNonNull( evaluation );

        // Get the geographic features
        Set<GeometryTuple> features = DeclarationUtilities.getFeatures( evaluation );

        // Get the atomic groups
        Set<Set<Metric>> atomicGroups = DeclarationUtilities.getMetricGroupsForProcessing( evaluation.metrics() );

        // Obtain the minimum sample size
        int minimumSampleSize = evaluation.minimumSampleSize();

        // Convert the threshold value units to the desired evaluation unit,
        // as required
        UnitMapper unitMapper = null;

        if ( Objects.nonNull( evaluation.unit() ) )
        {
            unitMapper = UnitMapper.of( evaluation.unit(),
                                        evaluation.unitAliases() );
        }

        // Iterate the groups
        Set<MetricsAndThresholds> metricsAndThresholds = new HashSet<>();
        for ( Set<Metric> nextGroup : atomicGroups )
        {
            // Gather the metric names
            Set<MetricConstants> names = nextGroup.stream()
                                                  .map( Metric::name )
                                                  .collect( Collectors.toUnmodifiableSet() );

            // Gather/wrap the thresholds and correlate them with feature tuples
            Map<FeatureTuple, Set<ThresholdOuter>> thresholds = new HashMap<>();

            // Default to the evaluation type, but override by metric group setting
            Pool.EnsembleAverageType ensembleAverageType = evaluation.ensembleAverageType();

            for ( Metric nextMetric : nextGroup )
            {
                MetricParameters nextParameters = nextMetric.parameters();

                if ( Objects.nonNull( nextParameters ) )
                {
                    Set<wres.config.yaml.components.Threshold> valueThresholds = nextParameters.thresholds();

                    // Convert units, as needed
                    valueThresholds = ThresholdSlicer.convertUnits( valueThresholds, unitMapper );

                    ThresholdSlicer.addThresholds( valueThresholds,
                                                   ThresholdType.VALUE,
                                                   thresholds,
                                                   features );
                    ThresholdSlicer.addThresholds( nextParameters.probabilityThresholds(),
                                                   ThresholdType.PROBABILITY,
                                                   thresholds,
                                                   features );
                    ThresholdSlicer.addThresholds( nextParameters.classifierThresholds(),
                                                   ThresholdType.PROBABILITY_CLASSIFIER,
                                                   thresholds,
                                                   features );

                    // Metric-specific ensemble average type?
                    if ( Objects.nonNull( nextParameters.ensembleAverageType() ) )
                    {
                        ensembleAverageType = nextParameters.ensembleAverageType();
                    }
                }
            }

            MetricsAndThresholds nextMetricsAndThresholds = new MetricsAndThresholds( names,
                                                                                      thresholds,
                                                                                      minimumSampleSize,
                                                                                      ensembleAverageType );
            metricsAndThresholds.add( nextMetricsAndThresholds );
        }

        return Collections.unmodifiableSet( metricsAndThresholds );
    }

    /**
     * Adds the supplied thresholds to the input map, correlating the features as needed.
     * @param thresholds the thresholds
     * @param type the threshold type
     * @param thresholdsToIncrement the threshold map to update
     * @param geometries the geometries to assist with feature correlation
     */
    private static void addThresholds( Set<wres.config.yaml.components.Threshold> thresholds,
                                       ThresholdType type,
                                       Map<FeatureTuple, Set<ThresholdOuter>> thresholdsToIncrement,
                                       Set<GeometryTuple> geometries )
    {
        // Up to many feature tuples per feature name: see issue #116312
        Map<String, List<GeometryTuple>> leftGeometries =
                geometries.stream()
                          .filter( GeometryTuple::hasLeft )
                          .collect( Collectors.groupingBy( n -> n.getLeft()
                                                                 .getName() ) );
        Map<String, List<GeometryTuple>> rightGeometries =
                geometries.stream()
                          .filter( GeometryTuple::hasRight )
                          .collect( Collectors.groupingBy( n -> n.getRight()
                                                                 .getName() ) );
        Map<String, List<GeometryTuple>> baselineGeometries =
                geometries.stream()
                          .filter( GeometryTuple::hasBaseline )
                          .collect( Collectors.groupingBy( n -> n.getBaseline()
                                                                 .getName() ) );

        BiFunction<Geometry, DatasetOrientation, List<GeometryTuple>> mapper = ( g, d ) ->
                switch ( d )
                {
                    case LEFT -> leftGeometries.getOrDefault( g.getName(), Collections.emptyList() );
                    case RIGHT -> rightGeometries.getOrDefault( g.getName(), Collections.emptyList() );
                    case BASELINE -> baselineGeometries.getOrDefault( g.getName(), Collections.emptyList() );
                    default -> throw new IllegalStateException( "Unrecognized dataset orientation in this "
                                                                + "context: " + d );
                };

        // Increment the thresholds
        for ( wres.config.yaml.components.Threshold nextThreshold : thresholds )
        {
            Geometry nextFeature = nextThreshold.feature();

            // If there is no geometry associated with the threshold, then add it for all geometries
            if ( Objects.isNull( nextFeature ) )
            {
                ThresholdSlicer.addThresholdForAllFeatures( nextThreshold, geometries, type, thresholdsToIncrement );
            }
            else
            {
                ThresholdSlicer.addThresholdForOneFeature( nextThreshold,
                                                           nextFeature,
                                                           mapper,
                                                           type,
                                                           thresholdsToIncrement );
            }
        }
    }

    /**
     * Adds the threshold to the supplied map for all available features.
     * @param nextThreshold the threshold to add
     * @param features the features to which the threshold applies
     * @param type the type of threshold
     * @param thresholdsToIncrement the thresholds to increment
     */

    private static void addThresholdForAllFeatures( wres.config.yaml.components.Threshold nextThreshold,
                                                    Set<GeometryTuple> features,
                                                    ThresholdType type,
                                                    Map<FeatureTuple, Set<ThresholdOuter>> thresholdsToIncrement )
    {
        for ( GeometryTuple nextTuple : features )
        {
            if ( Objects.nonNull( nextTuple ) )
            {
                FeatureTuple nextFeatureTuple = FeatureTuple.of( nextTuple );
                ThresholdOuter threshold = ThresholdOuter.of( nextThreshold.threshold(), type );
                if ( thresholdsToIncrement.containsKey( nextFeatureTuple ) )
                {
                    thresholdsToIncrement.get( nextFeatureTuple )
                                         .add( threshold );
                }
                else
                {
                    Set<ThresholdOuter> newSet = new HashSet<>();
                    newSet.add( threshold );
                    thresholdsToIncrement.put( nextFeatureTuple, newSet );
                }
            }
        }
    }

    /**
     * Adds the threshold to the supplied map for the feature provided.
     * @param nextThreshold the threshold to add
     * @param nextFeature the feature to which the threshold applies
     * @param mapper the mapper to find a feature tuple for the feature
     * @param type the type of threshold
     * @param thresholdsToIncrement the thresholds to increment
     */

    private static void addThresholdForOneFeature( wres.config.yaml.components.Threshold nextThreshold,
                                                   Geometry nextFeature,
                                                   BiFunction<Geometry, DatasetOrientation, List<GeometryTuple>> mapper,
                                                   ThresholdType type,
                                                   Map<FeatureTuple, Set<ThresholdOuter>> thresholdsToIncrement )
    {
        DatasetOrientation orientation = nextThreshold.featureNameFrom();

        if ( Objects.isNull( orientation ) )
        {
            throw new ThresholdException( "Failed to correlate a threshold with a feature tuple because the "
                                          + "threshold does not specify the orientation of the feature provided to "
                                          + "determine its tuple. The threshold is: "
                                          + nextThreshold
                                          + ". The feature name is: "
                                          + nextFeature.getName()
                                          + "." );
        }

        List<GeometryTuple> nextTuples = mapper.apply( nextFeature, orientation );
        if ( !nextTuples.isEmpty() )
        {
            for ( GeometryTuple nextTuple : nextTuples )
            {
                FeatureTuple nextFeatureTuple = FeatureTuple.of( nextTuple );
                ThresholdOuter threshold = ThresholdOuter.of( nextThreshold.threshold(), type );
                if ( thresholdsToIncrement.containsKey( nextFeatureTuple ) )
                {
                    thresholdsToIncrement.get( nextFeatureTuple )
                                         .add( threshold );
                }
                else
                {
                    Set<ThresholdOuter> newSet = new HashSet<>();
                    newSet.add( threshold );
                    thresholdsToIncrement.put( nextFeatureTuple, newSet );
                }
            }
        }
    }

    /**
     * Returns the basic event thresholds associated with a specified metric, eliminating the "all data" threshold
     * where appropriate.
     * @param metric the metric
     * @param thresholds the thresholds
     * @return the basic event thresholds
     */
    private static Set<ThresholdOuter> getBasicEventThresholdsForMetric( MetricConstants metric,
                                                                         Set<ThresholdOuter> thresholds )
    {
        Set<ThresholdOuter> nonClassifiers = thresholds.stream()
                                                       .filter( n -> n.getType()
                                                                     != ThresholdType.PROBABILITY_CLASSIFIER )
                                                       .collect( Collectors.toSet() );

        // Remove an all data threshold for a metric that does not require it
        if ( metric.isInGroup( SampleDataGroup.DISCRETE_PROBABILITY )
             || metric.isInGroup( SampleDataGroup.DICHOTOMOUS ) )
        {
            nonClassifiers.remove( ThresholdOuter.ALL_DATA );
        }

        return Collections.unmodifiableSet( nonClassifiers );
    }

    /**
     * @param builder the builder
     * @param values the values to set
     * @param probabilities the probabilities to set
     */

    private static void setValuesAndProbabilities( ThresholdOuter.Builder builder,
                                                   Set<OneOrTwoDoubles> values,
                                                   Set<OneOrTwoDoubles> probabilities )
    {
        boolean set = false;

        if ( values.size() == 1 )
        {
            builder.setValues( values.iterator()
                                     .next() );
            set = true;
        }

        if ( probabilities.size() == 1 )
        {
            builder.setProbabilities( probabilities.iterator()
                                                   .next() );
            set = true;
        }

        if ( !set )
        {
            if ( probabilities.size() > 1 )
            {
                // BETWEEN operator
                if ( probabilities.stream()
                                  .anyMatch( OneOrTwoDoubles::hasTwo ) )
                {
                    builder.setProbabilities( OneOrTwoDoubles.of( Double.NaN, Double.NaN ) );
                }
                else
                {
                    builder.setProbabilities( OneOrTwoDoubles.of( Double.NaN ) );
                }
            }
            else if ( values.size() > 1 )
            {
                // BETWEEN operator
                if ( values.stream()
                           .anyMatch( OneOrTwoDoubles::hasTwo ) )
                {
                    builder.setValues( OneOrTwoDoubles.of( Double.NaN, Double.NaN ) );
                }
                else
                {
                    builder.setValues( OneOrTwoDoubles.of( Double.NaN ) );
                }
            }
        }
    }

    /**
     * Converts the units of the supplied real-valued thresholds.
     * @param thresholds the real-valued thresholds
     * @param unitMapper the unit mapper
     * @return the thresholds with adjusted units
     */
    private static Set<wres.config.yaml.components.Threshold> convertUnits( Set<wres.config.yaml.components.Threshold> thresholds,
                                                                            UnitMapper unitMapper )
    {
        if ( Objects.isNull( unitMapper ) )
        {
            LOGGER.warn( "Could not perform any unit conversion of the real-valued threshold units as the desired "
                         + "measurement unit is undefined. Assuming that the desired units match the existing units." );

            return thresholds;
        }

        Set<wres.config.yaml.components.Threshold> adjusted = new HashSet<>();
        for ( wres.config.yaml.components.Threshold threshold : thresholds )
        {
            Threshold inner = threshold.threshold();

            // Do not convert the "all data" threshold
            ThresholdOuter allDataCheck = ThresholdOuter.of( inner );

            ThresholdBuilder builder = ThresholdBuilder.builder( threshold );
            if ( !allDataCheck.isAllDataThreshold()
                 && !Objects.equals( inner.getThresholdValueUnits(),
                                     unitMapper.getDesiredMeasurementUnitName() ) )
            {
                DoubleUnaryOperator mapper = unitMapper.getUnitMapper( inner.getThresholdValueUnits() );
                double leftValue = mapper.applyAsDouble( inner.getLeftThresholdValue() );

                Threshold.Builder innerAdjusted = inner.toBuilder()
                                                       .setLeftThresholdValue( leftValue )
                                                       .setThresholdValueUnits( unitMapper.getDesiredMeasurementUnitName() );

                if ( inner.hasRightThresholdValue() )
                {
                    double rightValue = mapper.applyAsDouble( inner.getRightThresholdValue() );
                    innerAdjusted.setRightThresholdValue( rightValue );
                }

                builder.threshold( innerAdjusted.build() );
            }

            adjusted.add( builder.build() );
        }

        return Collections.unmodifiableSet( adjusted );
    }

    /**
     * Do not construct.
     */

    private ThresholdSlicer()
    {
    }

}
