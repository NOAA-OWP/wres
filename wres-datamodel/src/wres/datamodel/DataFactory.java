package wres.datamodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold.MetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.outputs.MetricOutputMapByMetric.MetricOutputMapByMetricBuilder;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * A factory class for producing datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class DataFactory
{

    /**
     * Convenience method that returns a {@link Pair} to map a {@link MetricOutput} by {@link TimeWindow} and
     * {@link OneOrTwoThresholds}.
     * 
     * @param timeWindow the time window
     * @param values the values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a map key
     */

    public static Pair<TimeWindow, OneOrTwoThresholds> ofMapKeyByTimeThreshold( TimeWindow timeWindow,
                                                                                OneOrTwoDoubles values,
                                                                                Operator condition,
                                                                                ThresholdDataType dataType )
    {
        return Pair.of( timeWindow, OneOrTwoThresholds.of( Threshold.of( values, condition, dataType ) ) );
    }

    /**
     * Forms the union of the {@link PairedOutput}, returning a {@link PairedOutput} that contains all of the pairs in 
     * the inputs.
     * 
     * @param <S> the left side of the paired output
     * @param <T> the right side of the paired output
     * @param collection the list of inputs
     * @return a combined {@link PairedOutput}
     * @throws NullPointerException if the input is null
     */

    public static <S, T> PairedOutput<S, T> unionOf( Collection<PairedOutput<S, T>> collection )
    {
        Objects.requireNonNull( collection );
        List<Pair<S, T>> combined = new ArrayList<>();
        List<TimeWindow> combinedWindows = new ArrayList<>();
        MetricOutputMetadata sourceMeta = null;
        for ( PairedOutput<S, T> next : collection )
        {
            combined.addAll( next.getData() );
            if ( Objects.isNull( sourceMeta ) )
            {
                sourceMeta = next.getMetadata();
            }
            combinedWindows.add( next.getMetadata().getTimeWindow() );
        }
        TimeWindow unionWindow = null;
        if ( !combinedWindows.isEmpty() )
        {
            unionWindow = TimeWindow.unionOf( combinedWindows );
        }

        MetricOutputMetadata combinedMeta =
                MetricOutputMetadata.of( MetricOutputMetadata.of( sourceMeta, combined.size() ),
                                         unionWindow,
                                         sourceMeta.getThresholds() );
        return PairedOutput.of( combined, combinedMeta );
    }

    /**
     * Return a {@link Pair} from two double vectors.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    public static Pair<VectorOfDoubles, VectorOfDoubles> pairOf( double[] left, double[] right )
    {
        return new Pair<VectorOfDoubles, VectorOfDoubles>()
        {

            private static final long serialVersionUID = -1498961647587422087L;

            @Override
            public VectorOfDoubles setValue( VectorOfDoubles vectorOfDoubles )
            {
                throw new UnsupportedOperationException( "Cannot set on this entry." );
            }

            @Override
            public VectorOfDoubles getLeft()
            {
                return VectorOfDoubles.of( left );
            }

            @Override
            public VectorOfDoubles getRight()
            {
                return VectorOfDoubles.of( right );
            }
        };
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByTimeAndThreshold} of metric outputs
     */

    public static <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            ofMetricOutputMapByTimeAndThreshold( Map<Pair<TimeWindow, OneOrTwoThresholds>, T> input )
    {
        Objects.requireNonNull( input, "Specify a non-null map of inputs by lead time and threshold." );
        final MetricOutputMapByTimeAndThresholdBuilder<T> builder =
                new MetricOutputMapByTimeAndThresholdBuilder<>();
        input.forEach( builder::put );
        return builder.build();
    }

    /**
     * Returns a {@link MetricOutputMultiMapByTimeAndThreshold} from a map of inputs by {@link TimeWindow} and 
     * {@link OneOrTwoThresholds}.
     * 
     * @param <T> the type of output
     * @param input the input map of metric outputs by time window and threshold
     * @return a map of metric outputs by lead time and threshold for several metrics
     * @throws MetricOutputException if attempting to add multiple results for the same metric by time and threshold
     */

    public static <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThreshold<T>
            ofMetricOutputMultiMapByTimeAndThreshold( Map<Pair<TimeWindow, OneOrTwoThresholds>, List<MetricOutputMapByMetric<T>>> input )
    {
        Objects.requireNonNull( input, "Specify a non-null map of inputs by threshold." );
        final MetricOutputMultiMapByTimeAndThresholdBuilder<T> builder =
                new MetricOutputMultiMapByTimeAndThresholdBuilder<>();
        input.forEach( ( key, value ) -> {
            //Merge the outputs for different metrics
            final MetricOutputMapByMetricBuilder<T> mBuilder = new MetricOutputMapByMetricBuilder<>();
            value.forEach( mBuilder::put );
            builder.put( key, mBuilder.build() );
        } );
        return builder.build();
    }

    /**
     * Returns a builder for a {@link MetricOutputMultiMapByTimeAndThreshold} that allows for the incremental addition of
     * {@link MetricOutputMapByTimeAndThreshold} as they are computed.
     * 
     * @param <T> the type of output
     * @return a {@link MetricOutputMultiMapByTimeAndThresholdBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    public static <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThresholdBuilder<T>
            ofMetricOutputMultiMapByTimeAndThresholdBuilder()
    {
        return new MetricOutputMultiMapByTimeAndThresholdBuilder<>();
    }

    /**
     * Returns a builder for a {@link MetricOutputForProjectByTimeAndThreshold}.
     * 
     * @return a {@link MetricOutputForProjectByTimeAndThresholdBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    public static MetricOutputForProjectByTimeAndThresholdBuilder ofMetricOutputForProjectByTimeAndThreshold()
    {
        return new MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder();
    }

    /**
     * Returns a {@link MetricOutputMapByMetric} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByMetric} of metric outputs
     */

    public static <T extends MetricOutput<?>> MetricOutputMapByMetric<T>
            ofMetricOutputMapByMetric( Map<MetricConstants, T> input )
    {
        Objects.requireNonNull( input, "Specify a non-null list of inputs." );
        final MetricOutputMapByMetricBuilder<T> builder = new MetricOutputMapByMetricBuilder<>();
        input.forEach( ( key, value ) -> builder.put( MapKey.of( key ), value ) );
        return builder.build();
    }

    /**
     * Combines a list of {@link MetricOutputMapByTimeAndThreshold} into a single map.
     * 
     * @param <T> the type of output
     * @param input the list of input maps
     * @return a combined {@link MetricOutputMapByTimeAndThreshold} of metric outputs
     */

    public static <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            combine( List<MetricOutputMapByTimeAndThreshold<T>> input )
    {
        Objects.requireNonNull( input, "Specify a non-null map of inputs to combine." );
        final MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<T> builder =
                new MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<>();
        //If the input contains time windows, find the union of them
        List<TimeWindow> windows = new ArrayList<>();
        for ( MetricOutputMapByTimeAndThreshold<T> next : input )
        {
            next.forEach( builder::put );
            if ( next.getMetadata().hasTimeWindow() )
            {
                windows.add( next.getMetadata().getTimeWindow() );
            }
        }
        MetricOutputMetadata override = input.get( 0 ).getMetadata();
        if ( !windows.isEmpty() )
        {
            override = MetricOutputMetadata.of( override, TimeWindow.unionOf( windows ), null );
        }
        builder.setOverrideMetadata( override );
        return builder.build();
    }

    /**
     * Helper that checks for the equality of two double values using a prescribed number of significant digits.
     * 
     * @param first the first double
     * @param second the second double
     * @param digits the number of significant digits
     * @return true if the first and second are equal to the number of significant digits
     */

    public static boolean doubleEquals( double first, double second, int digits )
    {
        return Math.abs( first - second ) < 1.0 / digits;
    }

    /**
     * Consistent comparison of double arrays, first checks count of elements,
     * next goes through values.
     *
     * If first has fewer values, return -1, if first has more values, return 1.
     *
     * If value count is equal, go through in order until an element is less
     * or greater than another. If all values are equal, return 0.
     *
     * @param first the first array
     * @param second the second array
     * @return -1 if first is less than second, 0 if equal, 1 otherwise.
     */
    public static int compareDoubleArray( final double[] first,
                                          final double[] second )
    {
        // this one has fewer elements
        if ( first.length < second.length )
        {
            return -1;
        }
        // this one has more elements
        else if ( first.length > second.length )
        {
            return 1;
        }
        // compare values until we diverge
        else // assumption here is lengths are equal
        {
            for ( int i = 0; i < first.length; i++ )
            {
                int safeComparisonResult = Double.compare( first[i], second[i] );
                if ( safeComparisonResult != 0 )
                {
                    return safeComparisonResult;
                }
            }
            // all values were equal
            return 0;
        }
    }

    /**
     * Prevent construction.
     */

    private DataFactory()
    {
    }

}
