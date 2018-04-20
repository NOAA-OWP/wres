package wres.datamodel;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.SafeTimeSeriesOfEnsemblePairs.SafeTimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.SafeTimeSeriesOfSingleValuedPairs.SafeTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdType;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Default implementation of a utility class for slicing/dicing and transforming datasets associated with verification
 * metrics.
 * 
 * @author james.brown@hydrosolved.com
 */

class DefaultSlicer implements Slicer
{

    /**
     * Data factory for transformations.
     */

    private final DataFactory dataFac;

    /**
     * Instance of the slicer.
     */

    private static DefaultSlicer instance = null;

    /**
     * Returns an instance of a {@link Slicer}.
     * 
     * @return a {@link DataFactory}
     */

    public static Slicer getInstance()
    {
        if ( Objects.isNull( instance ) )
        {
            instance = new DefaultSlicer();
        }
        return instance;
    }

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

    @Override
    public double[] getLeftSide( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.getData().stream().mapToDouble( PairOfDoubles::getItemOne ).toArray();
    }

    @Override
    public double[] getLeftSide( EnsemblePairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.getData().stream().mapToDouble( PairOfDoubleAndVectorOfDoubles::getItemOne ).toArray();
    }

    @Override
    public double[] getRightSide( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.getData().stream().mapToDouble( PairOfDoubles::getItemTwo ).toArray();
    }

    @Override
    public SingleValuedPairs filter( SingleValuedPairs input,
                                     Predicate<PairOfDoubles> condition,
                                     DoublePredicate applyToClimatology )
    {

        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );

        List<PairOfDoubles> mainPairs = input.getData();
        List<PairOfDoubles> mainPairsSubset = mainPairs.stream().filter( condition ).collect( Collectors.toList() );

        //Filter climatology as required
        VectorOfDoubles climatology = input.getClimatology();
        if ( input.hasClimatology() && Objects.nonNull( applyToClimatology ) )
        {
            climatology =
                    this.filter( input.getClimatology(), applyToClimatology );
        }

        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            List<PairOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubles> basePairsSubset = basePairs.stream().filter( condition ).collect( Collectors.toList() );

            return dataFac.ofSingleValuedPairs( mainPairsSubset,
                                                basePairsSubset,
                                                input.getMetadata(),
                                                input.getMetadataForBaseline(),
                                                climatology );
        }

        return dataFac.ofSingleValuedPairs( mainPairsSubset, input.getMetadata(), climatology );
    }

    @Override
    public EnsemblePairs filter( EnsemblePairs input,
                                 Predicate<PairOfDoubleAndVectorOfDoubles> condition,
                                 DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );

        List<PairOfDoubleAndVectorOfDoubles> mainPairs = input.getData();
        List<PairOfDoubleAndVectorOfDoubles> mainPairsSubset =
                mainPairs.stream().filter( condition ).collect( Collectors.toList() );

        //Filter climatology as required
        VectorOfDoubles climatology = input.getClimatology();
        if ( input.hasClimatology() && Objects.nonNull( applyToClimatology ) )
        {
            climatology =
                    this.filter( input.getClimatology(), applyToClimatology );
        }

        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            List<PairOfDoubleAndVectorOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubleAndVectorOfDoubles> basePairsSubset =
                    basePairs.stream().filter( condition ).collect( Collectors.toList() );

            return dataFac.ofEnsemblePairs( mainPairsSubset,
                                            basePairsSubset,
                                            input.getMetadata(),
                                            input.getMetadataForBaseline(),
                                            climatology );
        }

        return dataFac.ofEnsemblePairs( mainPairsSubset, input.getMetadata(), climatology );
    }

    @Override
    public EnsemblePairs filter( EnsemblePairs input,
                                 Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubleAndVectorOfDoubles> mapper,
                                 DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<PairOfDoubleAndVectorOfDoubles> mainPairs = input.getData();
        List<PairOfDoubleAndVectorOfDoubles> mainPairsSubset = new ArrayList<>();

        for ( PairOfDoubleAndVectorOfDoubles next : mainPairs )
        {
            PairOfDoubleAndVectorOfDoubles transformed = mapper.apply( next );
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
                    this.filter( input.getClimatology(), applyToClimatology );
        }

        if ( input.hasBaseline() )
        {
            List<PairOfDoubleAndVectorOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubleAndVectorOfDoubles> basePairsSubset = new ArrayList<>();

            for ( PairOfDoubleAndVectorOfDoubles next : basePairs )
            {
                PairOfDoubleAndVectorOfDoubles transformed = mapper.apply( next );
                if ( Objects.nonNull( transformed ) )
                {
                    basePairsSubset.add( transformed );
                }
            }

            return dataFac.ofEnsemblePairs( mainPairsSubset,
                                            basePairsSubset,
                                            input.getMetadata(),
                                            input.getMetadataForBaseline(),
                                            climatology );
        }
        return dataFac.ofEnsemblePairs( mainPairsSubset, input.getMetadata(), climatology );
    }


    @Override
    public TimeSeriesOfSingleValuedPairs filter( TimeSeriesOfSingleValuedPairs input,
                                                 Predicate<TimeSeriesOfSingleValuedPairs> condition,
                                                 DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );

        TimeSeriesOfSingleValuedPairsBuilder builder = dataFac.ofTimeSeriesOfSingleValuedPairsBuilder();

        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );

        // Filter the main pairs and add them
        for ( TimeSeries<PairOfDoubles> next : input.basisTimeIterator() )
        {
            TimeSeriesOfSingleValuedPairs nextPair = (TimeSeriesOfSingleValuedPairs) next;
            if ( condition.test( nextPair ) )
            {
                builder.addTimeSeries( nextPair );
            }
        }

        //Filter climatology as required
        if ( input.hasClimatology() && Objects.nonNull( applyToClimatology ) )
        {
            VectorOfDoubles climatology =
                    this.filter( input.getClimatology(), applyToClimatology );

            builder.setClimatology( climatology );
        }

        //Filter baseline pairs as required
        if ( input.hasBaseline() )
        {
            builder.setMetadataForBaseline( input.getMetadataForBaseline() );

            for ( TimeSeries<PairOfDoubles> next : input.getBaselineData().basisTimeIterator() )
            {
                TimeSeriesOfSingleValuedPairs nextPair = (TimeSeriesOfSingleValuedPairs) next;
                if ( condition.test( nextPair ) )
                {
                    builder.addTimeSeriesForBaseline( nextPair );
                }
            }

        }

        return builder.build();
    }

    @Override
    public TimeSeriesOfSingleValuedPairs filterByDuration( TimeSeriesOfSingleValuedPairs input,
                                                           Predicate<Duration> duration )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );
        
        Objects.requireNonNull( duration, NULL_PREDICATE_EXCEPTION );

        //Iterate through the durations and append to the builder
        //Throw an exception if attempting to construct an irregular time-series
        SafeTimeSeriesOfSingleValuedPairsBuilder builder = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        
        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );
        
        for ( TimeSeries<PairOfDoubles> a : input.durationIterator() )
        {
            TimeSeriesOfSingleValuedPairs next = (TimeSeriesOfSingleValuedPairs) a;
            if ( duration.test( a.getDurations().first() ) )
            {
                builder.addTimeSeries( next );
            }
        }

        return builder.build();
    }

    @Override
    public TimeSeriesOfSingleValuedPairs filterByBasisTime( TimeSeriesOfSingleValuedPairs input,
                                                            Predicate<Instant> basisTime )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );
        
        Objects.requireNonNull( basisTime, NULL_PREDICATE_EXCEPTION );
        
        SafeTimeSeriesOfSingleValuedPairsBuilder builder = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        
        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );

        //Add the filtered data
        for ( TimeSeries<PairOfDoubles> a : input.basisTimeIterator() )
        {
            if ( basisTime.test( a.getEarliestBasisTime() ) )
            {
                builder.addTimeSeries( (TimeSeriesOfSingleValuedPairs) a );
            }
        }

        return builder.build();
    }

    @Override
    public TimeSeriesOfEnsemblePairs filterByDuration( TimeSeriesOfEnsemblePairs input, Predicate<Duration> condition )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );
        
        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );
        
        //Iterate through the durations and append to the builder
        //Throw an exception if attempting to construct an irregular time-series
        SafeTimeSeriesOfEnsemblePairsBuilder builder = new SafeTimeSeriesOfEnsemblePairsBuilder();
        
        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );
        
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> a : input.durationIterator() )
        {
            TimeSeriesOfEnsemblePairs next = (TimeSeriesOfEnsemblePairs) a;
            if ( condition.test( a.getDurations().first() ) )
            {
                builder.addTimeSeries( next );
            }
        }
        
        return builder.build();
    }

    @Override
    public TimeSeriesOfEnsemblePairs filterByBasisTime( TimeSeriesOfEnsemblePairs input, Predicate<Instant> condition )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );
        
        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );
        
        SafeTimeSeriesOfEnsemblePairsBuilder builder = new SafeTimeSeriesOfEnsemblePairsBuilder();
        
        // Set the metadata explicitly in case of an empty slice
        builder.setMetadata( input.getMetadata() );
        
        //Add the filtered data
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> a : input.basisTimeIterator() )
        {
            if ( condition.test( a.getEarliestBasisTime() ) )
            {
                builder.addTimeSeries( (TimeSeriesOfEnsemblePairs) a );
            }
        }

        return builder.build();
    }

    @Override
    public TimeSeriesOfEnsemblePairs filterByTraceIndex( TimeSeriesOfEnsemblePairs input, Predicate<Integer> condition )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );
        
        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );
        
        //Build a single-valued time-series with the trace at index currentTrace
        SafeTimeSeriesOfEnsemblePairsBuilder builder =
                new SafeTimeSeriesOfEnsemblePairsBuilder();
        builder.setMetadata( input.getMetadata() );
        DataFactory dFac = DefaultDataFactory.getInstance();
        
        //Iterate through the basis times
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> nextSeries : input.basisTimeIterator() )
        {
            List<Event<PairOfDoubleAndVectorOfDoubles>> rawInput = new ArrayList<>();

            //Iterate through the pairs
            for ( Event<PairOfDoubleAndVectorOfDoubles> next : nextSeries.timeIterator() )
            {
                //Reform the pairs with a subset of ensemble members
                double[] allTraces = next.getValue().getItemTwo();
                List<Double> subTraces = new ArrayList<>();
                for ( int i = 0; i < allTraces.length; i++ )
                {
                    if ( condition.test( i ) )
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
                                        dFac.pairOf( next.getValue().getItemOne(),
                                                     subTraces.toArray( new Double[subTraces.size()] ) ) ) );
            }
            builder.addTimeSeriesData( nextSeries.getEarliestBasisTime(), rawInput );
        }

        //Return the time-series
        return builder.build();
    }

    @Override
    public Map<Integer, List<PairOfDoubleAndVectorOfDoubles>>
            filterByRightSize( List<PairOfDoubleAndVectorOfDoubles> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.stream().collect( Collectors.groupingBy( pair -> pair.getItemTwo().length ) );
    }

    @Override
    public <T extends ScoreOutput<?, T>> Map<MetricConstants, MetricOutputMapByTimeAndThreshold<T>>
            filterByMetricComponent( MetricOutputMapByTimeAndThreshold<T> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Map<MetricConstants, Map<Pair<TimeWindow, OneOrTwoThresholds>, T>> sourceMap =
                new EnumMap<>( MetricConstants.class );
        input.forEach( ( key, value ) -> {
            Set<MetricConstants> components = value.getComponents();
            for ( MetricConstants next : components )
            {
                Map<Pair<TimeWindow, OneOrTwoThresholds>, T> nextMap = null;
                if ( sourceMap.containsKey( next ) )
                {
                    nextMap = sourceMap.get( next );
                }
                else
                {
                    nextMap = new HashMap<>();
                    sourceMap.put( next, nextMap );
                }
                //Add the output
                nextMap.put( key, value.getComponent( next ) );
            }
        } );
        //Build the score result
        Map<MetricConstants, MetricOutputMapByTimeAndThreshold<T>> returnMe =
                new EnumMap<>( MetricConstants.class );
        sourceMap.forEach( ( key, value ) -> returnMe.put( key,
                                                           dataFac.ofMetricOutputMapByTimeAndThreshold( value ) ) );
        return returnMe;
    }

    @Override
    public List<PairOfDoubles> transform( List<PairOfDoubleAndVectorOfDoubles> input,
                                          Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<PairOfDoubles> transformed = new ArrayList<>();
        input.stream().map( mapper ).forEach( transformed::add );
        return transformed;
    }

    @Override
    public DichotomousPairs transform( SingleValuedPairs input, Function<PairOfDoubles, PairOfBooleans> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<PairOfDoubles> mainPairs = input.getData();
        List<PairOfBooleans> mainPairsTransformed = new ArrayList<>();
        mainPairs.stream().map( mapper ).forEach( mainPairsTransformed::add );
        if ( input.hasBaseline() )
        {
            List<PairOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfBooleans> basePairsTransformed = new ArrayList<>();
            basePairs.stream().map( mapper ).forEach( basePairsTransformed::add );
            return dataFac.ofDichotomousPairsFromAtomic( mainPairsTransformed,
                                                         basePairsTransformed,
                                                         input.getMetadata(),
                                                         input.getMetadataForBaseline(),
                                                         input.getClimatology() );
        }
        return dataFac.ofDichotomousPairsFromAtomic( mainPairsTransformed,
                                                     input.getMetadata(),
                                                     input.getClimatology() );
    }

    @Override
    public SingleValuedPairs transform( EnsemblePairs input,
                                        Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<PairOfDoubles> mainPairsTransformed = transform( input.getData(), mapper );
        if ( input.hasBaseline() )
        {
            List<PairOfDoubles> basePairsTransformed = transform( input.getDataForBaseline(), mapper );
            return dataFac.ofSingleValuedPairs( mainPairsTransformed,
                                                basePairsTransformed,
                                                input.getMetadata(),
                                                input.getMetadataForBaseline(),
                                                input.getClimatology() );
        }
        return dataFac.ofSingleValuedPairs( mainPairsTransformed, input.getMetadata(), input.getClimatology() );
    }

    @Override
    public DiscreteProbabilityPairs transform( EnsemblePairs input,
                                               Threshold threshold,
                                               BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_MAPPER_EXCEPTION );

        List<PairOfDoubleAndVectorOfDoubles> mainPairs = input.getData();
        List<PairOfDoubles> mainPairsTransformed = new ArrayList<>();
        mainPairs.forEach( pair -> mainPairsTransformed.add( mapper.apply( pair, threshold ) ) );
        if ( input.hasBaseline() )
        {
            List<PairOfDoubleAndVectorOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubles> basePairsTransformed = new ArrayList<>();
            basePairs.forEach( pair -> basePairsTransformed.add( mapper.apply( pair, threshold ) ) );
            return dataFac.ofDiscreteProbabilityPairs( mainPairsTransformed,
                                                       basePairsTransformed,
                                                       input.getMetadata(),
                                                       input.getMetadataForBaseline(),
                                                       input.getClimatology() );
        }
        return dataFac.ofDiscreteProbabilityPairs( mainPairsTransformed, input.getMetadata(), input.getClimatology() );
    }

    @Override
    public PairOfDoubles transform( PairOfDoubleAndVectorOfDoubles pair, Threshold threshold )
    {
        Objects.requireNonNull( pair, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( threshold, NULL_INPUT_EXCEPTION );

        double rhs = Arrays.stream( pair.getItemTwo() ).map( a -> threshold.test( a ) ? 1 : 0 ).average().getAsDouble();
        return dataFac.pairOf( threshold.test( pair.getItemOne() ) ? 1 : 0, rhs );
    }

    @Override
    public Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> transform( ToDoubleFunction<double[]> transformer )
    {
        Objects.requireNonNull( transformer, NULL_INPUT_EXCEPTION );

        return pair -> dataFac.pairOf( pair.getItemOne(), transformer.applyAsDouble( pair.getItemTwo() ) );
    }

    @Override
    public Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubleAndVectorOfDoubles>
            leftAndEachOfRight( DoublePredicate predicate )
    {
        return pair -> {
            PairOfDoubleAndVectorOfDoubles returnMe = null;

            //Left meets condition
            if ( predicate.test( pair.getItemOne() ) )
            {
                double[] filtered = Arrays.stream( pair.getItemTwo() )
                                          .filter( predicate )
                                          .toArray();

                //One or more of right meets condition
                if ( filtered.length > 0 )
                {
                    returnMe = dataFac.pairOf( pair.getItemOne(), filtered );
                }
            }
            return returnMe;
        };
    }

    @Override
    public DoubleUnaryOperator getQuantileFunction( double[] sorted )
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

    @Override
    public Threshold getQuantileFromProbability( Threshold threshold, double[] sorted, Integer digits )
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
        return dataFac.ofQuantileThreshold( SafeOneOrTwoDoubles.of( first, second ),
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

    private VectorOfDoubles filter( VectorOfDoubles input, DoublePredicate condition )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( input, NULL_PREDICATE_EXCEPTION );

        double[] filtered = Arrays.stream( input.getDoubles() )
                                  .filter( condition )
                                  .toArray();

        return dataFac.vectorOf( filtered );
    }

    /**
     * Hidden constructor.
     */

    private DefaultSlicer()
    {
        dataFac = DefaultDataFactory.getInstance();
    }

}
