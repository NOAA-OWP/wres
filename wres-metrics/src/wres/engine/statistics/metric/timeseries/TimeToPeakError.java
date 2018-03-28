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
import wres.engine.statistics.metric.Incremental;
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
public class TimeToPeakError implements Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>,
        Incremental<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>>
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
            // Add the time-to-peak error against the basis time
            // Duration.between is negative if the predicted/right or "end" is before the observed/left or "start"
            returnMe.add( Pair.of( next.getEarliestBasisTime(), Duration.between( peakLeftTime, peakRightTime ) ) );
        }

        // Create output metadata with the identifier of the statistic as the component identifier
        Metadata in = s.getMetadata();
        Dimension outputDimension = getDataFactory().getMetadataFactory().getDimension( "DURATION" );
        MetricOutputMetadata meta = getDataFactory().getMetadataFactory().getOutputMetadata( s.getBasisTimes().size(),
                                                                                             outputDimension,
                                                                                             in.getDimension(),
                                                                                             getID(),
                                                                                             MetricConstants.MAIN,
                                                                                             in.getIdentifier(),
                                                                                             in.getTimeWindow() );

        return getDataFactory().ofPairedOutput( returnMe, meta );
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

    @Override
    public PairedOutput<Instant, Duration> combine( TimeSeriesOfSingleValuedPairs input,
                                                    PairedOutput<Instant, Duration> output )
    {
        // Validate
        if ( Objects.isNull( input ) )
        {
            throw new MetricInputException( "Specify non-null input to combine with '" + this + "'." );
        }
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null output to combine with '" + this + "'." );
        }
        // Add the earlier output to a new list
        List<PairedOutput<Instant, Duration>> combined = new ArrayList<>();
        combined.add( output );
        combined.add( apply( input ) );
        return getDataFactory().unionOf( combined );
    }

    @Override
    public PairedOutput<Instant, Duration> complete( PairedOutput<Instant, Duration> output )
    {
        return output;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class TimeToPeakErrorBuilder
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
        public TimeToPeakErrorBuilder setOutputFactory( final DataFactory dataFactory )
        {
            this.dataFactory = dataFactory;
            return this;
        }

        @Override
        public TimeToPeakError build() throws MetricParameterException
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
