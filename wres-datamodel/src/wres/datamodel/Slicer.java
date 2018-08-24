package wres.datamodel;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.pairs.DichotomousPair;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPair;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPairs;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.statistics.ListOfMetricOutput;
import wres.datamodel.statistics.MetricOutput;
import wres.datamodel.statistics.ScoreOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdType;
import wres.datamodel.time.BasicTimeSeries;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * A utility class for slicing/dicing and transforming datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class Slicer
{

    /**
     * Null input error message.
     */
    private static final String NULL_INPUT_EXCEPTION = "Specify a non-null input.";

    /**
     * Null mapper function error message.
     */

    private static final String NULL_MAPPER_EXCEPTION = "Specify a non-null function to map the input to an output.";

    /**
     * Failure to supply a non-null predicate.
     */

    private static final String NULL_PREDICATE_EXCEPTION = "Specify a non-null predicate.";

    /**
     * <p>Composes the input predicate as applying to the left side of a pair. 
     * 
     * <p>Also see {@link #filter(SingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<SingleValuedPair> left( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left." );

        return pair -> predicate.test( pair.getLeft() );
    }

    /**
     * <p>Composes the input predicate as applying to the right side of a pair.
     * 
     * <p>Also see {@link #filter(SingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<SingleValuedPair> right( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by right." );

        return pair -> predicate.test( pair.getRight() );
    }

    /**
     * <p>Composes the input predicate as applying to the both the left and right sides of a pair.
     * 
     * <p>Also see {@link #filter(SingleValuedPairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<SingleValuedPair> leftAndRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left and right." );

        return pair -> predicate.test( pair.getLeft() ) && predicate.test( pair.getRight() );
    }

    /**
     * <p>Composes the input predicate as applying to the either the left side or to the right side of a pair.
     * 
     * <p>Also see {@link #filter(SingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<SingleValuedPair> leftOrRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left or right." );

        return pair -> predicate.test( pair.getLeft() ) || predicate.test( pair.getRight() );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<EnsemblePair> leftVector( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left." );

        return pair -> predicate.test( pair.getLeft() );
    }

    /**
     * <p>Composes the input predicate as applying to all elements of the right side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<EnsemblePair> allOfRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by all of right." );

        return pair -> Arrays.stream( pair.getRight() ).allMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to one or more elements of the right side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<EnsemblePair> anyOfRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by any of right." );

        return pair -> Arrays.stream( pair.getRight() ).anyMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and all elements of the right side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<EnsemblePair> leftAndAllOfRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left and all of right." );

        return pair -> predicate.test( pair.getLeft() ) && Arrays.stream( pair.getRight() ).allMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and any element of the right side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<EnsemblePair> leftAndAnyOfRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left and any of right." );

        return pair -> predicate.test( pair.getLeft() ) && Arrays.stream( pair.getRight() ).anyMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the transformed value of the right side of a pair.</p>
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @param transformer the transformer
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<EnsemblePair> right( DoublePredicate predicate,
                                                 ToDoubleFunction<double[]> transformer )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by right." );

        Objects.requireNonNull( transformer, "Specify a non-null transformer when slicing by right." );

        return pair -> predicate.test( transformer.applyAsDouble( pair.getRight() ) );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and to the transformed value of the 
     * right side of a pair.</p>
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @param transformer the transformer
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<EnsemblePair> leftAndRight( DoublePredicate predicate,
                                                        ToDoubleFunction<double[]> transformer )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left and right." );

        Objects.requireNonNull( transformer, "Specify a non-null transformer when slicing by left and right." );

        return pair -> predicate.test( pair.getLeft() )
                       && predicate.test( transformer.applyAsDouble( pair.getRight() ) );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of any paired value within a time-series.
     * 
     * <p>Also see {@link #filter(TimeSeriesOfSingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<TimeSeries<SingleValuedPair>>
            anyOfLeftInTimeSeriesOfSingleValuedPairs( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing a time-series by any of left." );

        return times -> {

            // Iterate the times
            for ( Event<SingleValuedPair> next : times.timeIterator() )
            {
                // Condition is met for one time
                if ( predicate.test( next.getValue().getLeft() ) )
                {
                    return true;
                }
            }

            return false;
        };
    }

    /**
     * <p>Composes the input predicate as applying to the right side of any paired value within a time-series.
     * 
     * <p>Also see {@link #filter(TimeSeriesOfSingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<TimeSeries<SingleValuedPair>>
            anyOfRightInTimeSeriesOfSingleValuedPairs( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing a time-series by any of right." );

        return times -> {

            // Iterate the times
            for ( Event<SingleValuedPair> next : times.timeIterator() )
            {
                // Condition is met for one time
                if ( predicate.test( next.getValue().getRight() ) )
                {
                    return true;
                }
            }

            return false;
        };
    }

    /**
     * <p>Composes the input predicate as applying to the left side of any paired value within a time-series and,
     * separately, to the right side of any paired value within that time-series.</p>
     * 
     * <p>Also see {@link #filter(TimeSeriesOfSingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<TimeSeries<SingleValuedPair>>
            anyOfLeftAndAnyOfRightInTimeSeriesOfSingleValuedPairs( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate,
                                "Specify non-null input when slicing a time-series by any of left"
                                           + "and any of right." );

        // Compose independently
        return times -> Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( predicate ).test( times )
                        && Slicer.anyOfRightInTimeSeriesOfSingleValuedPairs( predicate ).test( times );
    }

    /**
     * Loops over the {@link SingleValuedPair} in the input and returns <code>true</code> when a pair is encountered
     * for which the {@link Predicate} returns <code>true</code> for both sides of the pairing, false otherwise. 
     * 
     * @param pairs the input pairs
     * @param predicate the predicate to apply
     * @return true if one or more inputs meet the predicate condition on both sides of the pairing, false otherwise
     * @throws NullPointerException if either input is null
     */

    public static boolean hasOneOrMoreOf( List<SingleValuedPair> pairs, DoublePredicate predicate )
    {

        Objects.requireNonNull( pairs, "Specify non-null pairs when checking for one or more of the input." );

        Objects.requireNonNull( predicate, "Specify non-null predicate when checking for one or more of the input." );

        for ( SingleValuedPair next : pairs )
        {
            if ( predicate.test( next.getLeft() ) && predicate.test( next.getRight() ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Loops over the {@link EnsemblePair} in the input and returns <code>true</code> when a pair 
     * is encountered for which the {@link Predicate} returns <code>true</code> for the left side of the pairing and
     * for one or more values on the right side, false otherwise. 
     * 
     * @param pairs the input pairs
     * @param predicate the predicate to apply
     * @return true if one or more inputs meet the predicate condition on both sides of the pairing, false otherwise
     * @throws NullPointerException if either input is null
     */

    public static boolean hasOneOrMoreOfVectorRight( List<EnsemblePair> pairs,
                                                     DoublePredicate predicate )
    {
        Objects.requireNonNull( pairs, "Specify non-null pairs when checking for one or more of the input." );

        Objects.requireNonNull( predicate, "Specify non-null predicate when checking for one or more of the input." );

        for ( EnsemblePair next : pairs )
        {
            if ( predicate.test( next.getLeft() ) && Arrays.stream( next.getRight() ).anyMatch( predicate ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a {@link Threshold} with quantiles defined from the prescribed {@link Threshold} with probabilities, 
     * where the quantiles are mapped using {@link #getQuantileFunction(double[])}.
     * 
     * @param sorted the sorted input array
     * @param threshold the probability threshold from which the quantile is determined
     * @return the quantile threshold
     * @throws NullPointerException if either input is null
     */

    public static Threshold getQuantileFromProbability( Threshold threshold, double[] sorted )
    {
        Objects.requireNonNull( threshold, "Specify a non-null probability whose quantile value is required." );

        Objects.requireNonNull( sorted, "Specify a non-null array of sorted values to determine the quantile." );

        return Slicer.getQuantileFromProbability( threshold, sorted, null );
    }

    /**
     * A transformer that applies a predicate to the left and each of the right separately, returning a transformed
     * pair or null if the left and none of the right meet the condition.
     * 
     * @param predicate the input predicate
     * @return a composed function
     */

    public static Function<EnsemblePair, EnsemblePair>
            leftAndEachOfRight( DoublePredicate predicate )
    {
        return pair -> {
            EnsemblePair returnMe = null;

            //Left meets condition
            if ( predicate.test( pair.getLeft() ) )
            {
                double[] filtered = Arrays.stream( pair.getRight() )
                                          .filter( predicate )
                                          .toArray();

                //One or more of right meets condition
                if ( filtered.length > 0 )
                {
                    returnMe = EnsemblePair.of( pair.getLeft(), filtered );
                }
            }
            return returnMe;
        };
    }

    /**
     * Returns the left side of a {@link SingleValuedPairs} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the left side
     * @throws NullPointerException if the input is null
     */

    public static double[] getLeftSide( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.getRawData().stream().mapToDouble( SingleValuedPair::getLeft ).toArray();
    }

    /**
     * Returns the right side of a {@link SingleValuedPairs} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the right side
     * @throws NullPointerException if the input is null
     */

    public static double[] getRightSide( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.getRawData().stream().mapToDouble( SingleValuedPair::getRight ).toArray();
    }

    /**
     * Returns the left side of a {@link EnsemblePairs} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the left side
     * @throws NullPointerException if the input is null
     */

    public static double[] getLeftSide( EnsemblePairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.getRawData().stream().mapToDouble( EnsemblePair::getLeft ).toArray();
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * Does not modify the metadata associated with the input.
     * 
     * @param input the pairs to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static SingleValuedPairs filter( SingleValuedPairs input,
                                            Predicate<SingleValuedPair> condition,
                                            DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );

        List<SingleValuedPair> mainPairs = input.getRawData();
        List<SingleValuedPair> mainPairsSubset = mainPairs.stream().filter( condition ).collect( Collectors.toList() );

        //Filter climatology as required
        VectorOfDoubles climatology = input.getClimatology();
        if ( input.hasClimatology() && Objects.nonNull( applyToClimatology ) )
        {
            climatology =
                    Slicer.filter( input.getClimatology(), applyToClimatology );
        }

        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            List<SingleValuedPair> basePairs = input.getRawDataForBaseline();
            List<SingleValuedPair> basePairsSubset =
                    basePairs.stream().filter( condition ).collect( Collectors.toList() );

            return SingleValuedPairs.of( mainPairsSubset,
                                         basePairsSubset,
                                         input.getMetadata(),
                                         input.getMetadataForBaseline(),
                                         climatology );
        }

        return SingleValuedPairs.of( mainPairsSubset, input.getMetadata(), climatology );
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * Does not modify the metadata associated with the input.
     * 
     * @param input the pairs to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static EnsemblePairs filter( EnsemblePairs input,
                                        Predicate<EnsemblePair> condition,
                                        DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );

        List<EnsemblePair> mainPairs = input.getRawData();
        List<EnsemblePair> mainPairsSubset =
                mainPairs.stream().filter( condition ).collect( Collectors.toList() );

        //Filter climatology as required
        VectorOfDoubles climatology = input.getClimatology();
        if ( input.hasClimatology() && Objects.nonNull( applyToClimatology ) )
        {
            climatology =
                    Slicer.filter( input.getClimatology(), applyToClimatology );
        }

        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            List<EnsemblePair> basePairs = input.getRawDataForBaseline();
            List<EnsemblePair> basePairsSubset =
                    basePairs.stream().filter( condition ).collect( Collectors.toList() );

            return EnsemblePairs.of( mainPairsSubset,
                                     basePairsSubset,
                                     input.getMetadata(),
                                     input.getMetadataForBaseline(),
                                     climatology );
        }

        return EnsemblePairs.of( mainPairsSubset, input.getMetadata(), climatology );
    }

    /**
     * Filters {@link EnsemblePairs} by applying a mapper function to the input. This allows for fine-grain filtering
     * of specific elements of the right side of a pair. For example, filter all elements of the right side that 
     * correspond to {@link Double#isNaN()}. Does not modify the metadata associated with the input.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to a new {@link EnsemblePairs}
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the filtered {@link EnsemblePairs}
     * @throws NullPointerException if either the input or condition is null
     */

    public static EnsemblePairs filter( EnsemblePairs input,
                                        Function<EnsemblePair, EnsemblePair> mapper,
                                        DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<EnsemblePair> mainPairs = input.getRawData();
        List<EnsemblePair> mainPairsSubset = new ArrayList<>();

        for ( EnsemblePair next : mainPairs )
        {
            EnsemblePair transformed = mapper.apply( next );
            if ( Objects.nonNull( transformed ) )
            {
                mainPairsSubset.add( transformed );
            }
        }

        //Filter climatology as required
        VectorOfDoubles climatology = input.getClimatology();
        if ( input.hasClimatology() && Objects.nonNull( applyToClimatology ) )
        {
            climatology =
                    Slicer.filter( input.getClimatology(), applyToClimatology );
        }

        if ( input.hasBaseline() )
        {
            List<EnsemblePair> basePairs = input.getRawDataForBaseline();
            List<EnsemblePair> basePairsSubset = new ArrayList<>();

            for ( EnsemblePair next : basePairs )
            {
                EnsemblePair transformed = mapper.apply( next );
                if ( Objects.nonNull( transformed ) )
                {
                    basePairsSubset.add( transformed );
                }
            }

            return EnsemblePairs.of( mainPairsSubset,
                                     basePairsSubset,
                                     input.getMetadata(),
                                     input.getMetadataForBaseline(),
                                     climatology );
        }
        return EnsemblePairs.of( mainPairsSubset, input.getMetadata(), climatology );
    }

    /**
     * Returns a subset of pairs where the condition is met for each atomic time-series in the container. Applies to 
     * both the main pairs and any baseline pairs. Does not modify the metadata associated with the input.
     * 
     * @param input the pairs to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static TimeSeriesOfSingleValuedPairs filter( TimeSeriesOfSingleValuedPairs input,
                                                        Predicate<TimeSeries<SingleValuedPair>> condition,
                                                        DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );

        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();

        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );

        // Filter the main pairs and add them
        for ( TimeSeries<SingleValuedPair> next : input.basisTimeIterator() )
        {
            if ( condition.test( next ) )
            {
                builder.addTimeSeries( next );
            }
        }

        //Filter climatology as required
        if ( input.hasClimatology() && Objects.nonNull( applyToClimatology ) )
        {
            VectorOfDoubles climatology =
                    Slicer.filter( input.getClimatology(), applyToClimatology );

            builder.setClimatology( climatology );
        }

        //Filter baseline pairs as required
        if ( input.hasBaseline() )
        {
            builder.setMetadataForBaseline( input.getMetadataForBaseline() );

            for ( TimeSeries<SingleValuedPair> next : input.getBaselineData().basisTimeIterator() )
            {
                if ( condition.test( next ) )
                {
                    builder.addTimeSeriesForBaseline( next );
                }
            }

        }

        return builder.build();
    }

    /**
     * Filters the input time-series by the {@link Duration} associated with each pair. Applies to both the main pairs 
     * and any baseline pairs. Does not modify the metadata associated with the input.
     * 
     * @param input the pairs to slice
     * @param duration the duration condition on which to slice
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static TimeSeriesOfSingleValuedPairs filterByDuration( TimeSeriesOfSingleValuedPairs input,
                                                                  Predicate<Duration> duration )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( duration, NULL_PREDICATE_EXCEPTION );

        //Iterate through the durations and append to the builder
        //Throw an exception if attempting to construct an irregular time-series
        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();

        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );

        for ( TimeSeries<SingleValuedPair> a : input.durationIterator() )
        {
            if ( duration.test( a.getDurations().first() ) )
            {
                builder.addTimeSeries( a );
            }
        }

        return builder.build();
    }

    /**
     * Filters the input time-series by basis time. Applies to both the main pairs and any baseline pairs. Does not 
     * modify the metadata associated with the input.
     * 
     * @param input the pairs to slice
     * @param basisTime the basis time condition on which to slice
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static TimeSeriesOfSingleValuedPairs filterByBasisTime( TimeSeriesOfSingleValuedPairs input,
                                                                   Predicate<Instant> basisTime )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( basisTime, NULL_PREDICATE_EXCEPTION );

        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();

        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );

        //Add the filtered data
        for ( TimeSeries<SingleValuedPair> a : input.basisTimeIterator() )
        {
            if ( basisTime.test( a.getEarliestBasisTime() ) )
            {
                builder.addTimeSeries( a );
            }
        }

        return builder.build();
    }

    /**
     * Filters the input time-series by the {@link Duration} associated with each pair. Applies to both the main pairs 
     * and any baseline pairs. Does not modify the metadata associated with the input.
     * 
     * @param input the pairs to slice
     * @param condition the condition on which to slice
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static TimeSeriesOfEnsemblePairs filterByDuration( TimeSeriesOfEnsemblePairs input,
                                                              Predicate<Duration> condition )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );

        //Iterate through the durations and append to the builder
        //Throw an exception if attempting to construct an irregular time-series
        TimeSeriesOfEnsemblePairsBuilder builder = new TimeSeriesOfEnsemblePairsBuilder();

        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );

        for ( TimeSeries<EnsemblePair> a : input.durationIterator() )
        {
            if ( condition.test( a.getDurations().first() ) )
            {
                builder.addTimeSeries( a );
            }
        }

        return builder.build();
    }

    /**
     * Filters the input time-series by the {@link Duration} associated with each value. Does not modify the metadata 
     * associated with the input.
     * 
     * @param <T> the type of time-series data
     * @param input the input to slice
     * @param duration the duration condition on which to slice
     * @return the subset of the input that meets the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static <T> TimeSeries<T> filterByDuration( TimeSeries<T> input, Predicate<Duration> duration )
    {
        List<Event<List<Event<T>>>> returnMe = new ArrayList<>();
        // Iterate through basis times
        for ( TimeSeries<T> nextSeries : input.basisTimeIterator() )
        {
            Instant basisTime = nextSeries.getEarliestBasisTime();
            // Iterate through durations until it is later than the nextDuration
            for ( Event<T> nextEvent : nextSeries.timeIterator() )
            {
                Duration candidateDuration = Duration.between( basisTime, nextEvent.getTime() );
                if ( duration.test( candidateDuration ) )
                {
                    returnMe.add( Event.of( basisTime, Arrays.asList( nextEvent ) ) );
                }
            }
        }
        return BasicTimeSeries.of( returnMe );
    }

    /**
     * Filters the input time-series by basis time. Applies to both the main pairs and any baseline pairs. Does not 
     * modify the metadata associated with the input.
     * 
     * @param input the pairs to slice
     * @param basisTime the basis time condition on which to slice
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static TimeSeriesOfEnsemblePairs filterByBasisTime( TimeSeriesOfEnsemblePairs input,
                                                               Predicate<Instant> basisTime )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( basisTime, NULL_PREDICATE_EXCEPTION );

        TimeSeriesOfEnsemblePairsBuilder builder = new TimeSeriesOfEnsemblePairsBuilder();

        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );

        //Add the filtered data
        for ( TimeSeries<EnsemblePair> a : input.basisTimeIterator() )
        {
            if ( basisTime.test( a.getEarliestBasisTime() ) )
            {
                builder.addTimeSeries( a );
            }
        }

        return builder.build();
    }

    /**
     * Returns a {@link TimeSeries} whose elements are filtered according to the zero-based index of the ensemble trace 
     * or null if no such time-series exist.
     * 
     * @param input the pairs to slice
     * @param traceIndex the trace index filter
     * @return a time-series associated with a specific trace index or null
     * @throws NullPointerException if either input is null
     */

    public static TimeSeriesOfEnsemblePairs filterByTraceIndex( TimeSeriesOfEnsemblePairs input,
                                                                Predicate<Integer> traceIndex )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( traceIndex, NULL_PREDICATE_EXCEPTION );

        //Build a single-valued time-series with the trace at index currentTrace
        TimeSeriesOfEnsemblePairsBuilder builder =
                new TimeSeriesOfEnsemblePairsBuilder();
        builder.setMetadata( input.getMetadata() );

        //Iterate through the basis times
        for ( TimeSeries<EnsemblePair> nextSeries : input.basisTimeIterator() )
        {
            List<Event<EnsemblePair>> rawInput = new ArrayList<>();

            //Iterate through the pairs
            for ( Event<EnsemblePair> next : nextSeries.timeIterator() )
            {
                //Reform the pairs with a subset of ensemble members
                double[] allTraces = next.getValue().getRight();
                List<Double> subTraces = new ArrayList<>();
                for ( int i = 0; i < allTraces.length; i++ )
                {
                    if ( traceIndex.test( i ) )
                    {
                        subTraces.add( allTraces[i] );
                    }
                }
                //All time-series have the same number of ensemble members, 
                //so the first instance with no members means no traces
                if ( subTraces.isEmpty() )
                {
                    return null;
                }
                rawInput.add( Event.of( next.getTime(),
                                        EnsemblePair.of( next.getValue().getLeft(),
                                                         subTraces.toArray( new Double[subTraces.size()] ) ) ) );
            }
            builder.addTimeSeriesData( nextSeries.getEarliestBasisTime(), rawInput );
        }

        //Return the time-series
        return builder.build();
    }

    /**
     * <p>Returns a subset of metric outputs whose {@link MetricOutputMetadata} matches the supplied predicate. For 
     * example, to filter by a particular {@link TimeWindow} and {@link OneOrTwoThresholds} associated with the 
     * output metadata:</p>
     * 
     * <p><code>Slicer.filter( list, a {@literal ->} a.getTimeWindow().equals( someWindow ) {@literal &&} 
     *              a.getThresholds().equals( someThreshold ) );</code></p>
     *              
     * @param <T> the output type
     * @param outputs the outputs to filter
     * @param predicate the predicate to use as a filter
     * @return a filtered list whose elements meet the predicate supplied
     * @throws NullPointerException if the input list is null or the predicate is null
     */

    public static <T extends MetricOutput<?>> ListOfMetricOutput<T> filter( ListOfMetricOutput<T> outputs,
                                                                            Predicate<MetricOutputMetadata> predicate )
    {
        Objects.requireNonNull( outputs, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( predicate, NULL_INPUT_EXCEPTION );
        
        List<T> results = new ArrayList<>();

        // Filter
        for ( T next : outputs )
        {
            if ( predicate.test( next.getMetadata() ) )
            {
                results.add( next );
            }
        }

        return ListOfMetricOutput.of( Collections.unmodifiableList( results ) );
    }

    /**
     * <p>Discovers the unique instances of a given type associated with a {@link ListOfMetricOutput}. The mapper 
     * function identifies the type to discover. For example, to discover the unique thresholds contained in the list of
     * outputs:</p>
     * 
     * <p><code>Slicer.discover( outputs, next {@literal ->} next.getMetadata().getThresholds() );</code></p>
     * 
     * <p>To discover the unique metrics contained in the list of outputs:</p>
     * 
     * <p><code>Slicer.discover( outputs, next {@literal ->} next.getMetadata().getMetricID() );</code></p>
     * 
     * <p>To discover the unique pairs of lead times in the list of outputs:</p>
     * 
     * <p><code>Slicer.discover( outputs, next {@literal ->} 
     * Pair.of( next.getMetadata().getTimeWindow().getEarliestLeadTime(), 
     * next.getMetadata().getTimeWindow().getLatestLeadTime() );</code></p>
     * 
     * <p>Returns the empty set if no elements are mapped.</p>
     * 
     * @param <S> the metric output type
     * @param <T> the type of information required about the output
     * @param outputs the list of outputs
     * @param mapper the mapper function that discovers the type of interest
     * @return the unique instances of a given type associated with the output
     * @throws NullPointerException if the input list is null or the mapper is null
     */

    public static <S extends MetricOutput<?>, T extends Object> SortedSet<T> discover( ListOfMetricOutput<S> outputs,
                                                                                       Function<S, T> mapper )
    {
        Objects.requireNonNull( outputs, NULL_INPUT_EXCEPTION );
        
        Objects.requireNonNull( mapper, NULL_INPUT_EXCEPTION );
        
        return Collections.unmodifiableSortedSet( outputs.getData()
                                                         .stream()
                                                         .map( mapper )
                                                         .filter( Objects::nonNull )
                                                         .collect( Collectors.toCollection( TreeSet::new ) ) );
    }

    /**
     * <p>Convenience method that returns the metric output in the store whose identifier matches the 
     * input. This is equivalent to:</p>
     * 
     * <p><code>Slicer.filter( out, meta {@literal ->} meta.getMetricID() == metricIdentifier )</code></p>
     * 
     * @param <T> the metric output type
     * @param outputs the list of outputs
     * @param metricIdentifier the metric identifier                     
     * @throws NullPointerException if the input list is null or the identifier is null
     * @return the first available output that matches the input identifier or null if no such output is available
     */

    public static <T extends MetricOutput<?>> ListOfMetricOutput<T> filter( ListOfMetricOutput<T> outputs,
                                                                            MetricConstants metricIdentifier )
    {
        Objects.requireNonNull( outputs, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( metricIdentifier, NULL_INPUT_EXCEPTION );

        return Slicer.filter( outputs, meta -> meta.getMetricID() == metricIdentifier );
    }

    /**
     * Returns as many lists of {@link EnsemblePair} as groups of atomic pairs in the input with an
     * equal number of elements. i.e. each list of {@link EnsemblePair} in the returned result has an
     * equal number of elements, internally, and a different number of elements than all other subsets of pairs. The
     * subsets are returned in a map, indexed by the number of elements on the right side.
     * 
     * @param input a list of {@link EnsemblePair} to slice
     * @return as many subsets of {@link EnsemblePair} as groups of pairs in the input of equal size
     * @throws NullPointerException if the input is null
     */

    public static Map<Integer, List<EnsemblePair>>
            filterByRightSize( List<EnsemblePair> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.stream().collect( Collectors.groupingBy( pair -> pair.getRight().length ) );
    }

    /**
     * Returns a map of {@link ScoreOutput} for each component in the input map of {@link ScoreOutput}. The slices are 
     * mapped to their {@link MetricConstants} component identifier.
     * 
     * @param <T> the score component type
     * @param input the input map
     * @return the input map sliced by component identifier
     * @throws NullPointerException if the input is null
     */

    public static <T extends ScoreOutput<?, T>> Map<MetricConstants, ListOfMetricOutput<T>>
            filterByMetricComponent( ListOfMetricOutput<T> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Map<MetricConstants, ListOfMetricOutput<T>> returnMe = new EnumMap<>( MetricConstants.class );
        
        // Find the components
        SortedSet<MetricConstants> components = new TreeSet<>();
        input.forEach( next -> components.addAll( next.getComponents() ) );
        
        // Loop the components
        for ( MetricConstants nextComponent : components )
        {
            List<T> listOfComponent = new ArrayList<>();
            // Loop the entries
            for( T nextItem : input )
            {
                if( nextItem.hasComponent( nextComponent ) )
                {
                    listOfComponent.add( nextItem.getComponent( nextComponent ) );
                }
            }
            returnMe.put( nextComponent, ListOfMetricOutput.of( listOfComponent ) );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Produces a {@link List} of {@link SingleValuedPair} from a {@link List} of {@link EnsemblePair}
     * using a prescribed mapper function.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link SingleValuedPairs}
     * @return the {@link SingleValuedPairs}
     * @throws NullPointerException if either input is null
     */

    public static List<SingleValuedPair> toSingleValuedPairs( List<EnsemblePair> input,
                                                              Function<EnsemblePair, SingleValuedPair> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<SingleValuedPair> transformed = new ArrayList<>();
        input.stream().map( mapper ).forEach( transformed::add );
        return transformed;
    }

    /**
     * Produces {@link DichotomousPairs} from a {@link SingleValuedPairs} by applying a mapper function to the input.
     * 
     * @param input the {@link SingleValuedPairs} pairs
     * @param mapper the function that maps from {@link SingleValuedPairs} to {@link DichotomousPairs}
     * @return the {@link DichotomousPairs}
     * @throws NullPointerException if either input is null
     */

    public static DichotomousPairs toDichotomousPairs( SingleValuedPairs input,
                                                       Function<SingleValuedPair, DichotomousPair> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<SingleValuedPair> mainPairs = input.getRawData();
        List<DichotomousPair> mainPairsTransformed = new ArrayList<>();
        mainPairs.stream().map( mapper ).forEach( mainPairsTransformed::add );
        if ( input.hasBaseline() )
        {
            List<SingleValuedPair> basePairs = input.getRawDataForBaseline();
            List<DichotomousPair> basePairsTransformed = new ArrayList<>();
            basePairs.stream().map( mapper ).forEach( basePairsTransformed::add );
            return DichotomousPairs.ofDichotomousPairs( mainPairsTransformed,
                                                        basePairsTransformed,
                                                        input.getMetadata(),
                                                        input.getMetadataForBaseline(),
                                                        input.getClimatology() );
        }
        return DichotomousPairs.ofDichotomousPairs( mainPairsTransformed,
                                                    input.getMetadata(),
                                                    input.getClimatology() );
    }

    /**
     * Produces {@link DichotomousPairs} from a {@link DiscreteProbabilityPairs} by applying a mapper function to the 
     * input.
     * 
     * @param input the {@link SingleValuedPairs} pairs
     * @param mapper the function that maps from {@link SingleValuedPairs} to {@link DichotomousPairs}
     * @return the {@link DichotomousPairs}
     * @throws NullPointerException if either input is null
     */

    public static DichotomousPairs toDichotomousPairs( DiscreteProbabilityPairs input,
                                                       Function<DiscreteProbabilityPair, DichotomousPair> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<DiscreteProbabilityPair> mainPairs = input.getRawData();
        List<DichotomousPair> mainPairsTransformed = new ArrayList<>();
        mainPairs.stream().map( mapper ).forEach( mainPairsTransformed::add );
        if ( input.hasBaseline() )
        {
            List<DiscreteProbabilityPair> basePairs = input.getRawDataForBaseline();
            List<DichotomousPair> basePairsTransformed = new ArrayList<>();
            basePairs.stream().map( mapper ).forEach( basePairsTransformed::add );
            return DichotomousPairs.ofDichotomousPairs( mainPairsTransformed,
                                                        basePairsTransformed,
                                                        input.getMetadata(),
                                                        input.getMetadataForBaseline(),
                                                        input.getClimatology() );
        }
        return DichotomousPairs.ofDichotomousPairs( mainPairsTransformed,
                                                    input.getMetadata(),
                                                    input.getClimatology() );
    }

    /**
     * Produces {@link SingleValuedPairs} from a {@link EnsemblePairs} by applying a mapper function to the input.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link SingleValuedPairs} pairs
     * @return the {@link SingleValuedPairs}
     * @throws NullPointerException if either input is null
     */

    public static SingleValuedPairs toSingleValuedPairs( EnsemblePairs input,
                                                         Function<EnsemblePair, SingleValuedPair> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<SingleValuedPair> mainPairsTransformed = toSingleValuedPairs( input.getRawData(), mapper );
        if ( input.hasBaseline() )
        {
            List<SingleValuedPair> basePairsTransformed = toSingleValuedPairs( input.getRawDataForBaseline(), mapper );
            return SingleValuedPairs.of( mainPairsTransformed,
                                         basePairsTransformed,
                                         input.getMetadata(),
                                         input.getMetadataForBaseline(),
                                         input.getClimatology() );
        }
        return SingleValuedPairs.of( mainPairsTransformed, input.getMetadata(), input.getClimatology() );
    }

    /**
     * Produces {@link SingleValuedPairs} from a {@link DiscreteProbabilityPairs}.
     * 
     * @param input the {@link DiscreteProbabilityPairs}
     * @return the {@link SingleValuedPairs}
     * @throws NullPointerException if either input is null
     */

    public static SingleValuedPairs toSingleValuedPairs( DiscreteProbabilityPairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        if ( input.hasBaseline() )
        {
            return SingleValuedPairs.of( new ArrayList<SingleValuedPair>( input.getRawData() ),
                                         new ArrayList<SingleValuedPair>( input.getRawDataForBaseline() ),
                                         input.getMetadata(),
                                         input.getMetadataForBaseline(),
                                         input.getClimatology() );
        }
        return SingleValuedPairs.of( new ArrayList<SingleValuedPair>( input.getRawData() ),
                                     input.getMetadata(),
                                     input.getClimatology() );
    }

    /**
     * Produces {@link DiscreteProbabilityPairs} from a {@link EnsemblePairs} by applying a mapper function to the input
     * using a prescribed {@link Threshold}. See {@link #toDiscreteProbabilityPair(EnsemblePair, Threshold)}
     * for the mapper.
     * 
     * @param input the {@link EnsemblePairs}
     * @param threshold the {@link Threshold} used to transform the pairs
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link DiscreteProbabilityPairs}
     * @return the {@link DiscreteProbabilityPairs}
     * @throws NullPointerException if any input is null
     */

    public static DiscreteProbabilityPairs toDiscreteProbabilityPairs( EnsemblePairs input,
                                                                       Threshold threshold,
                                                                       BiFunction<EnsemblePair, Threshold, DiscreteProbabilityPair> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<EnsemblePair> mainPairs = input.getRawData();
        List<DiscreteProbabilityPair> mainPairsTransformed = new ArrayList<>();
        mainPairs.forEach( pair -> mainPairsTransformed.add( mapper.apply( pair, threshold ) ) );
        if ( input.hasBaseline() )
        {
            List<EnsemblePair> basePairs = input.getRawDataForBaseline();
            List<DiscreteProbabilityPair> basePairsTransformed = new ArrayList<>();
            basePairs.forEach( pair -> basePairsTransformed.add( mapper.apply( pair, threshold ) ) );
            return DiscreteProbabilityPairs.of( mainPairsTransformed,
                                                basePairsTransformed,
                                                input.getMetadata(),
                                                input.getMetadataForBaseline(),
                                                input.getClimatology() );
        }
        return DiscreteProbabilityPairs.of( mainPairsTransformed,
                                            input.getMetadata(),
                                            input.getClimatology() );
    }

    /**
     * Converts a {@link EnsemblePair} to a {@link DiscreteProbabilityPair} that contains the probabilities that
     * a discrete event occurs according to the left side and the right side, respectively. The event is represented by
     * a {@link Threshold}.
     * 
     * @param pair the pair to transform
     * @param threshold the threshold
     * @return the transformed pair
     * @throws NullPointerException if either input is null
     */

    public static DiscreteProbabilityPair toDiscreteProbabilityPair( EnsemblePair pair, Threshold threshold )
    {
        Objects.requireNonNull( pair, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( threshold, NULL_INPUT_EXCEPTION );

        double rhs = Arrays.stream( pair.getRight() ).map( a -> threshold.test( a ) ? 1 : 0 ).average().getAsDouble();
        return DiscreteProbabilityPair.of( threshold.test( pair.getLeft() ) ? 1 : 0, rhs );
    }

    /**
     * Returns a function that converts a {@link EnsemblePair} to a {@link SingleValuedPair} by 
     * applying the specified transformer to the {@link EnsemblePair#getRight()}.
     * 
     * @param transformer the transformer
     * @return a composed function
     * @throws NullPointerException if the input is null
     */

    public static Function<EnsemblePair, SingleValuedPair>
            ofSingleValuedPairMapper( ToDoubleFunction<double[]> transformer )
    {
        Objects.requireNonNull( transformer, NULL_INPUT_EXCEPTION );

        return pair -> SingleValuedPair.of( pair.getLeft(), transformer.applyAsDouble( pair.getRight() ) );
    }

    /**
     * Returns a function to compute a value from the sorted array that corresponds to the input non-exceedence 
     * probability. This method produces undefined results if the input array is unsorted. Corresponds to 
     * method 6 in the R function, quantile{stats}: <a href=
     * "https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html">https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html</a>.
     * Also see: <a href=
     * "https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample">https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample</a>.
     * 
     * @param sorted the sorted input array
     * @return the threshold
     * @throws NullPointerException if the input is null
     */

    public static DoubleUnaryOperator getQuantileFunction( double[] sorted )
    {
        return probability -> {
            if ( probability < 0 || probability > 1 )
            {
                throw new IllegalArgumentException( "The input probability is not within the unit interval: "
                                                    + probability );
            }
            if ( sorted.length == 0 )
            {
                return Double.NaN;
            }
            //Single item
            if ( sorted.length == 1 )
            {
                return sorted[0];
            }

            //Estimate the position
            double pos = probability * ( sorted.length + 1.0 );
            //Lower bound
            if ( pos < 1.0 )
            {
                return sorted[0];
            }
            //Upper bound
            else if ( pos >= sorted.length )
            {
                return sorted[sorted.length - 1];
            }
            //Contained: use linear interpolation
            else
            {
                double floorPos = Math.floor( pos );
                double dif = pos - floorPos;
                int intPos = (int) floorPos;
                double lower = sorted[intPos - 1];
                double upper = sorted[intPos];
                return lower + dif * ( upper - lower );
            }
        };
    }

    /**
     * Returns a {@link Threshold} with quantiles defined from a prescribed {@link Threshold} with probabilities, 
     * where the quantiles are mapped using {@link #getQuantileFunction(double[])}.
     * 
     * @param sorted the sorted input array
     * @param threshold the probability threshold from which the quantile threshold is determined
     * @param digits an optional number of decimal places to which the threshold will be rounded up (may be null)
     * @return the probability threshold
     * @throws NullPointerException if either input is null
     */

    public static Threshold getQuantileFromProbability( Threshold threshold, double[] sorted, Integer digits )
    {
        Objects.requireNonNull( threshold, "Specify a non-null probability threshold." );

        Objects.requireNonNull( sorted, "Specify a non-null array of sorted values." );

        if ( threshold.getType() != ThresholdType.PROBABILITY_ONLY )
        {
            throw new IllegalArgumentException( "The input threshold must be a probability threshold." );
        }
        if ( sorted.length == 0 )
        {
            throw new IllegalArgumentException( "Cannot compute the quantile from empty input." );
        }
        DoubleUnaryOperator qF = getQuantileFunction( sorted );
        Double first = qF.applyAsDouble( threshold.getProbabilities().first() );
        if ( Objects.nonNull( digits ) )
        {
            first = round().apply( first, digits );
        }
        Double second = null;
        if ( threshold.hasBetweenCondition() )
        {
            second = qF.applyAsDouble( threshold.getProbabilities().second() );
            if ( Objects.nonNull( digits ) )
            {
                second = round().apply( second, digits );
            }
        }
        return Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( first, second ),
                                              threshold.getProbabilities(),
                                              threshold.getCondition(),
                                              threshold.getDataType(),
                                              threshold.getLabel(),
                                              threshold.getUnits() );
    }

    /**
     * Rounds the input to the prescribed number of decimal places using {@link BigDecimal#ROUND_HALF_UP}.
     * 
     * @return a function that rounds to a prescribed number of decimal places
     */

    private static BiFunction<Double, Integer, Double> round()
    {
        return ( input, digits ) -> {
            BigDecimal bd = new BigDecimal( Double.toString( input ) ); //Always use String constructor
            bd = bd.setScale( digits, BigDecimal.ROUND_HALF_UP );
            return bd.doubleValue();
        };
    }

    /**
     * Filters a {@link VectorOfDoubles}, returning a subset whose elements meet the condition.
     * 
     * @param input the input
     * @param condition the condition
     * @return the filtered vector
     */

    private static VectorOfDoubles filter( VectorOfDoubles input, DoublePredicate condition )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( input, NULL_PREDICATE_EXCEPTION );

        double[] filtered = Arrays.stream( input.getDoubles() )
                                  .filter( condition )
                                  .toArray();

        return VectorOfDoubles.of( filtered );
    }

    /**
     * Hidden constructor.
     */

    private Slicer()
    {
    }

}
