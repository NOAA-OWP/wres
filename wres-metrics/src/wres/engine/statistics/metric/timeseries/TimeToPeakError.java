package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Constructs a {@link Metric} that returns the difference in time between the maximum values recorded in the left
 * and right side of each time-series in the {@link TimeSeriesOfSingleValuedPairs}. For multiple peaks with the same
 * value, the peak with the latest {@link Instant} is chosen. The timing error is measured with a {@link Duration}. A
 * negative {@link Duration} indicates that the predicted peak is after the observed peak.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public class TimeToPeakError extends Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
{

    @Override
    public PairedOutput<Instant, Duration> apply( TimeSeriesOfSingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        // Iterate through the time-series by basis time, and find the peaks in left and right
        List<Pair<Instant, Duration>> returnMe = new ArrayList<>();
        for ( TimeSeries<PairOfDoubles> next : s.basisTimeIterator() )
        {
            Instant peakLeftTime = null;
            Instant peakRightTime = null;
            double peakLeftValue = Double.NEGATIVE_INFINITY;
            double peakRightValue = Double.NEGATIVE_INFINITY;
            // Iterate through the pairs to find the peak on each side
            for ( Pair<Instant, PairOfDoubles> nextPair : next.timeIterator() )
            {
                // New peak left
                if ( Double.compare( nextPair.getRight().getItemOne(), peakLeftValue ) > 0 )
                {
                    peakLeftValue = nextPair.getRight().getItemOne();
                    peakLeftTime = nextPair.getLeft();
                }
                // New peak right
                if ( Double.compare( nextPair.getRight().getItemTwo(), peakRightValue ) > 0 )
                {
                    peakRightValue = nextPair.getRight().getItemTwo();
                    peakRightTime = nextPair.getLeft();
                }
            }
            // Add the time-to-peak error against the basis time
            // Duration.between is negative if the predicted/right or "end" is before the observed/left or "start"
            returnMe.add( Pair.of( next.getEarliestBasisTime(), Duration.between( peakLeftTime, peakRightTime ) ) );
        }
        final MetricOutputMetadata metOut = getMetadata( s, s.getBasisTimes().size(), MetricConstants.MAIN, null );
        return getDataFactory().ofPairedOutput( returnMe, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.TIME_TO_PEAK_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class TimeToPeakErrorBuilder
            extends MetricBuilder<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
    {
        @Override
        protected TimeToPeakError build() throws MetricParameterException
        {
            return new TimeToPeakError( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     * @throws MetricInputException if the input is invalid
     */

    private TimeToPeakError( final TimeToPeakErrorBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}
