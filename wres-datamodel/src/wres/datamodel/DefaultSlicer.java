package wres.datamodel;

import java.math.BigDecimal;
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
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;

/**
 * Default implementation of a utility class for slicing/dicing and transforming datasets associated with verification
 * metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
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
    private static final String NULL_INPUT = "Specify a non-null input to transform.";

    /**
     * Null mapper function error message.
     */

    private static final String NULL_MAPPER = "Specify a non-null function to map the input to an output.";

    @Override
    public double[] getLeftSide( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        return input.getData().stream().mapToDouble( PairOfDoubles::getItemOne ).toArray();
    }

    @Override
    public double[] getLeftSide( EnsemblePairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        return input.getData().stream().mapToDouble( PairOfDoubleAndVectorOfDoubles::getItemOne ).toArray();
    }

    @Override
    public double[] getRightSide( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        return input.getData().stream().mapToDouble( PairOfDoubles::getItemTwo ).toArray();
    }

    @Override
    public SingleValuedPairs filter( SingleValuedPairs input, DoublePredicate condition, boolean applyToClimatology )
            throws MetricInputSliceException
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Objects.requireNonNull( condition, "Specify a non-null condition." );
        String sliceFail = "While slicing, the condition failed to select any data.";
        List<PairOfDoubles> mainPairs = input.getData();
        List<PairOfDoubles> mainPairsSubset = new ArrayList<>();
        mainPairs.forEach( a -> {
            if ( condition.test( a.getItemOne() ) && condition.test( a.getItemTwo() ) )
                mainPairsSubset.add( a );
        } );
        //No pairs in the subset
        if ( mainPairsSubset.isEmpty() )
        {
            throw new MetricInputSliceException( sliceFail + " No data for main pairs." );
        }
        //Filter climatology as required
        VectorOfDoubles climatology = input.getClimatology();
        if ( input.hasClimatology() && applyToClimatology )
        {
            climatology = filter( input.getClimatology(), condition, sliceFail + " No data for climatology." );
        }
        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            List<PairOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubles> basePairsSubset = new ArrayList<>();
            basePairs.forEach( a -> {
                if ( condition.test( a.getItemOne() ) && condition.test( a.getItemTwo() ) )
                    basePairsSubset.add( a );
            } );
            //No pairs in the subset
            if ( basePairsSubset.isEmpty() )
            {
                throw new MetricInputSliceException( sliceFail + " No data for baseline pairs." );
            }
            return dataFac.ofSingleValuedPairs( mainPairsSubset,
                                                basePairsSubset,
                                                input.getMetadata(),
                                                input.getMetadataForBaseline(),
                                                climatology );
        }
        return dataFac.ofSingleValuedPairs( mainPairsSubset, input.getMetadata(), climatology );
    }

    @Override
    public EnsemblePairs filter( EnsemblePairs input, DoublePredicate condition, boolean applyToClimatology )
            throws MetricInputSliceException
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Objects.requireNonNull( condition, "Specify a non-null condition." );
        String sliceFail = "While slicing, the condition failed to select any data.";
        List<PairOfDoubleAndVectorOfDoubles> mainPairs = input.getData();
        List<PairOfDoubleAndVectorOfDoubles> mainPairsSubset = new ArrayList<>();
        mainPairs.forEach( a -> {
            PairOfDoubleAndVectorOfDoubles next = filter( a, condition );
            if ( Objects.nonNull( next ) )
            {
                mainPairsSubset.add( next );
            }
        } );
        //No pairs in the subset
        if ( mainPairsSubset.isEmpty() )
        {
            throw new MetricInputSliceException( sliceFail + " No data for main pairs." );
        }
        //Filter climatology as required
        VectorOfDoubles climatology = input.getClimatology();
        if ( input.hasClimatology() && applyToClimatology )
        {
            climatology = filter( input.getClimatology(), condition, sliceFail + " No data for climatology." );
        }
        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            List<PairOfDoubleAndVectorOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubleAndVectorOfDoubles> basePairsSubset = new ArrayList<>();
            basePairs.forEach( a -> {
                PairOfDoubleAndVectorOfDoubles next = filter( a, condition );
                if ( Objects.nonNull( next ) )
                {
                    basePairsSubset.add( next );
                }
            } );

            //No pairs in the subset
            if ( basePairsSubset.isEmpty() )
            {
                throw new MetricInputSliceException( sliceFail + " No data for baseline pairs." );
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
    public SingleValuedPairs filterByLeft( SingleValuedPairs input, Threshold threshold )
            throws MetricInputSliceException
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Objects.requireNonNull( threshold, "Specify a non-null threshold." );
        String sliceFail = "While slicing, the threshold '" + threshold + "' failed to select any data.";
        List<PairOfDoubles> mainPairs = input.getData();
        List<PairOfDoubles> mainPairsSubset = new ArrayList<>();
        mainPairs.forEach( a -> {
            if ( threshold.test( a.getItemOne() ) )
                mainPairsSubset.add( a );
        } );
        //No pairs in the subset
        if ( mainPairsSubset.isEmpty() )
        {
            throw new MetricInputSliceException( sliceFail );
        }
        if ( input.hasBaseline() )
        {
            List<PairOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubles> basePairsSubset = new ArrayList<>();
            basePairs.forEach( a -> {
                if ( threshold.test( a.getItemOne() ) )
                    basePairsSubset.add( a );
            } );

            //No pairs in the subset
            if ( basePairsSubset.isEmpty() )
            {
                throw new MetricInputSliceException( sliceFail );
            }
            return dataFac.ofSingleValuedPairs( mainPairsSubset,
                                                basePairsSubset,
                                                input.getMetadata(),
                                                input.getMetadataForBaseline(),
                                                input.getClimatology() );
        }
        return dataFac.ofSingleValuedPairs( mainPairsSubset, input.getMetadata(), input.getClimatology() );
    }

    @Override
    public EnsemblePairs filterByLeft( EnsemblePairs input, Threshold threshold ) throws MetricInputSliceException
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Objects.requireNonNull( threshold, "Specify a non-null threshold." );
        String sliceFail = "While slicing, the threshold '" + threshold + "' failed to select any data.";
        List<PairOfDoubleAndVectorOfDoubles> mainPairs = input.getData();
        List<PairOfDoubleAndVectorOfDoubles> mainPairsSubset = new ArrayList<>();
        mainPairs.forEach( a -> {
            if ( threshold.test( a.getItemOne() ) )
                mainPairsSubset.add( a );
        } );
        //No pairs in the subset
        if ( mainPairsSubset.isEmpty() )
        {
            throw new MetricInputSliceException( sliceFail );
        }
        if ( input.hasBaseline() )
        {
            List<PairOfDoubleAndVectorOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubleAndVectorOfDoubles> basePairsSubset = new ArrayList<>();
            basePairs.forEach( a -> {
                if ( threshold.test( a.getItemOne() ) )
                    basePairsSubset.add( a );
            } );
            //No pairs in the subset
            if ( basePairsSubset.isEmpty() )
            {
                throw new MetricInputSliceException( sliceFail );
            }
            return dataFac.ofEnsemblePairs( mainPairsSubset,
                                            basePairsSubset,
                                            input.getMetadata(),
                                            input.getMetadataForBaseline(),
                                            input.getClimatology() );
        }
        return dataFac.ofEnsemblePairs( mainPairsSubset, input.getMetadata(), input.getClimatology() );
    }

    @Override
    public Map<Integer, List<PairOfDoubleAndVectorOfDoubles>>
            filterByRight( List<PairOfDoubleAndVectorOfDoubles> input )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        return input.stream().collect( Collectors.groupingBy( pair -> pair.getItemTwo().length ) );
    }

    @Override
    public <T extends ScoreOutput<?,T>> Map<MetricConstants, MetricOutputMapByTimeAndThreshold<T>>
            filterByMetricComponent( MetricOutputMapByTimeAndThreshold<T> input )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Map<MetricConstants, Map<Pair<TimeWindow, Thresholds>, T>> sourceMap =
                new EnumMap<>( MetricConstants.class );
        input.forEach( ( key, value ) -> {
            Set<MetricConstants> components = value.getComponents();
            for ( MetricConstants next : components )
            {
                Map<Pair<TimeWindow, Thresholds>, T> nextMap = null;
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
        sourceMap.forEach( ( key, value ) -> returnMe.put( key, dataFac.ofMap( value ) ) );
        return returnMe;
    }

    @Override
    public List<PairOfDoubles> transformPairs( List<PairOfDoubleAndVectorOfDoubles> input,
                                               Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Objects.requireNonNull( mapper, NULL_MAPPER );
        List<PairOfDoubles> transformed = new ArrayList<>();
        input.stream().map( mapper ).forEach( transformed::add );
        return transformed;
    }

    @Override
    public DichotomousPairs transformPairs( SingleValuedPairs input, Function<PairOfDoubles, PairOfBooleans> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Objects.requireNonNull( mapper, NULL_MAPPER );
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
    public SingleValuedPairs transformPairs( EnsemblePairs input,
                                             Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Objects.requireNonNull( mapper, NULL_MAPPER );
        List<PairOfDoubles> mainPairsTransformed = transformPairs( input.getData(), mapper );
        if ( input.hasBaseline() )
        {
            List<PairOfDoubles> basePairsTransformed = transformPairs( input.getDataForBaseline(), mapper );
            return dataFac.ofSingleValuedPairs( mainPairsTransformed,
                                                basePairsTransformed,
                                                input.getMetadata(),
                                                input.getMetadataForBaseline(),
                                                input.getClimatology() );
        }
        return dataFac.ofSingleValuedPairs( mainPairsTransformed, input.getMetadata(), input.getClimatology() );
    }

    @Override
    public DiscreteProbabilityPairs transformPairs( EnsemblePairs input,
                                                    Threshold threshold,
                                                    BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper )
    {
        Objects.requireNonNull( input, NULL_INPUT );
        Objects.requireNonNull( mapper, NULL_MAPPER );
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
    public PairOfDoubles transformPair( PairOfDoubleAndVectorOfDoubles pair, Threshold threshold )
    {
        Objects.requireNonNull( pair, NULL_INPUT );
        Objects.requireNonNull( threshold, NULL_INPUT );
        double rhs = Arrays.stream( pair.getItemTwo() ).map( a -> threshold.test( a ) ? 1 : 0 ).average().getAsDouble();
        return dataFac.pairOf( threshold.test( pair.getItemOne() ) ? 1 : 0, rhs );
    }

    @Override
    public PairOfDoubles transformPair( PairOfDoubleAndVectorOfDoubles pair )
    {
        Objects.requireNonNull( pair, NULL_INPUT );
        return dataFac.pairOf( pair.getItemOne(), pair.getItemTwo()[0] );
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
                throw new IllegalArgumentException( "Cannot compute the quantile from empty input." );
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
        if ( !threshold.hasProbabilityValues() )
        {
            throw new IllegalArgumentException( "The input threshold must contain probability values." );
        }
        if ( sorted.length == 0 )
        {
            throw new IllegalArgumentException( "Cannot compute the quantile from empty input." );
        }
        DoubleUnaryOperator qF = getQuantileFunction( sorted );
        Double first = qF.applyAsDouble( threshold.getThresholdProbability() );
        if ( Objects.nonNull( digits ) )
        {
            first = round().apply( first, digits );
        }
        Double second = null;
        if ( threshold.hasBetweenCondition() )
        {
            second = qF.applyAsDouble( threshold.getThresholdUpperProbability() );
            if ( Objects.nonNull( digits ) )
            {
                second = round().apply( second, digits );
            }
        }
        return dataFac.ofQuantileThreshold( first,
                                            second,
                                            threshold.getThresholdProbability(),
                                            threshold.getThresholdUpperProbability(),
                                            threshold.getCondition(),
                                            threshold.getLabel() );
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
     * @param failMessage a message to locate a source of failure
     * @return the filtered vector
     * @throws MetricInputSliceException if no elements meet the condition
     */

    private VectorOfDoubles filter( VectorOfDoubles input, DoublePredicate condition, String failMessage )
            throws MetricInputSliceException
    {
        Objects.requireNonNull( input, "Specify non-null input to filter." );
        Objects.requireNonNull( input, "Specify a non-null condition on which to filter." );
        double[] filtered = Arrays.stream( input.getDoubles() )
                                  .filter( condition )
                                  .toArray();
        if ( filtered.length == 0 )
        {
            throw new MetricInputSliceException( failMessage );
        }
        return dataFac.vectorOf( filtered );
    }

    /**
     * Filters a {@link PairOfDoubleAndVectorOfDoubles}, returning a filtered pair that contains the left and subset of
     * right that meet the condition or null if the left or all of right do not meet the condition.
     * 
     * @param input the input pair
     * @param condition the filter
     * @return the filtered pair or null
     */

    private PairOfDoubleAndVectorOfDoubles filter( PairOfDoubleAndVectorOfDoubles input, DoublePredicate condition )
    {
        PairOfDoubleAndVectorOfDoubles returnMe = null;
        //Left meets condition
        if ( condition.test( input.getItemOne() ) )
        {
            double[] filtered = Arrays.stream( input.getItemTwo() )
                                      .filter( condition )
                                      .toArray();
            //One or more of right meets condition
            if ( filtered.length > 0 )
            {
                returnMe = dataFac.pairOf( input.getItemOne(), filtered );
            }
        }
        return returnMe;
    }

    /**
     * Hidden constructor.
     */

    private DefaultSlicer()
    {
        dataFac = DefaultDataFactory.getInstance();
    }

}
