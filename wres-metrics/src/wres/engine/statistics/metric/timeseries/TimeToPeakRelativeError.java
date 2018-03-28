package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.DataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * <p>Constructs a {@link Metric} that returns the fractional difference in time between the maximum values recorded in 
 * the left and right side of each time-series in the {@link TimeSeriesOfSingleValuedPairs}. The denominator in the 
 * fraction is given by the period, in hours, between the basis time and the time at which the maximum value is 
 * recorded in the left side of the paired input. Thus, for forecast time-series, the output is properly interpreted 
 * as the number of hours of error per hour of forecast lead time until the observed peak occurred.</p>
 * 
 * <p>For multiple peaks with the same value, the peak with the latest {@link Instant} is chosen. The timing error is 
 * measured with a {@link Duration}. However, the fraction is measured in relative hours, i.e. the timing error 
 * is divided by a <code>long</code> value of hours using {@link Duration#dividedBy(long)}. A negative {@link Duration} 
 * indicates that the predicted peak is after the observed peak.</p>
 * 
 * <p><b>Implementation Notes:</b></p>
 * 
 * <p>There is no value in this class overriding {@link TimeToPeakError} or implementing {@link Collectable} with that 
 * class because the denominator used to compute the relative error is different for each time-series in the input and 
 * this information is not available in the output from {@link TimeToPeakError}. TODO: there may be some value in
 * composing both classes with a shared abstraction to minimize overlaps.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeToPeakRelativeError implements Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
{

    /**
     * The data factory.
     */

    private final DataFactory dataFactory;

    @Override
    public DataFactory getDataFactory()
    {
        return dataFactory;
    }

    @Override
    public String toString()
    {
        return getID().toString();
    }

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
            for ( Event<PairOfDoubles> nextPair : next.timeIterator() )
            {
                // New peak left
                if ( Double.compare( nextPair.getValue().getItemOne(), peakLeftValue ) > 0 )
                {
                    peakLeftValue = nextPair.getValue().getItemOne();
                    peakLeftTime = nextPair.getTime();
                }
                // New peak right
                if ( Double.compare( nextPair.getValue().getItemTwo(), peakRightValue ) > 0 )
                {
                    peakRightValue = nextPair.getValue().getItemTwo();
                    peakRightTime = nextPair.getTime();
                }
            }
            // Compute the denominator
            Duration denominator = Duration.between( peakLeftTime, next.getEarliestBasisTime() );
            long denominatorHours = denominator.toHours();

            // Add the relative time-to-peak error against the basis time
            // Duration.between is negative if the predicted/right or "end" is before the observed/left or "start"
            returnMe.add( Pair.of( next.getEarliestBasisTime(),
                                   Duration.between( peakLeftTime, peakRightTime ).dividedBy( denominatorHours ) ) );
        }

        // Create output metadata with the identifier of the statistic as the component identifier
        Metadata in = s.getMetadata();
        Dimension outputDimension = getDataFactory().getMetadataFactory().getDimension( "DURATION IN RELATIVE HOURS" );
        MetricOutputMetadata meta = getDataFactory().getMetadataFactory().getOutputMetadata( s.getBasisTimes().size(),
                                                                                             outputDimension,
                                                                                             in.getDimension(),
                                                                                             this.getID(),
                                                                                             MetricConstants.MAIN,
                                                                                             in.getIdentifier(),
                                                                                             in.getTimeWindow() );

        return getDataFactory().ofPairedOutput( returnMe, meta );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class TimeToPeakRelativeErrorBuilder
            implements MetricBuilder<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
    {

        /**
         * The data factory.
         */

        private DataFactory dataFactory;

        /**
         * Sets the {@link DataFactory} for constructing a {@link MetricOutput}.
         * 
         * @param dataFactory the {@link DataFactory}
         * @return the builder
         */

        @Override
        public TimeToPeakRelativeErrorBuilder setOutputFactory( final DataFactory dataFactory )
        {
            this.dataFactory = dataFactory;
            return this;
        }

        @Override
        public TimeToPeakRelativeError build() throws MetricParameterException
        {
            return new TimeToPeakRelativeError( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     * @throws MetricInputException if the input is invalid
     */

    protected TimeToPeakRelativeError( final TimeToPeakRelativeErrorBuilder builder ) throws MetricParameterException
    {
        if ( Objects.isNull( builder ) )
        {
            throw new MetricParameterException( "Cannot construct the metric with a null builder." );
        }

        this.dataFactory = builder.dataFactory;

        if ( Objects.isNull( this.dataFactory ) )
        {
            throw new MetricParameterException( "Specify a data factory with which to build the metric." );
        }
    }

}
