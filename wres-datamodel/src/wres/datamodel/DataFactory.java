package wres.datamodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProject;
import wres.datamodel.outputs.MetricOutputForProject.MetricOutputForProjectBuilder;
import wres.datamodel.outputs.PairedOutput;
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
     * Returns a builder for a {@link MetricOutputForProject}.
     * 
     * @return a {@link MetricOutputForProjectBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    public static MetricOutputForProjectBuilder ofMetricOutputForProjectByTimeAndThreshold()
    {
        return new MetricOutputForProject.MetricOutputForProjectBuilder();
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
