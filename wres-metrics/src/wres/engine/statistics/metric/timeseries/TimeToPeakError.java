package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.Dimension;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * <p>Constructs a {@link Metric} that returns the difference in time between the maximum values recorded in the left
 * and right side of each time-series in the {@link TimeSeriesOfSingleValuedPairs}. For multiple peaks with the same
 * value, the peak with the latest {@link Instant} is chosen. The timing error is measured with a {@link Duration}. A
 * negative {@link Duration} indicates that the predicted peak is after the observed peak.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeToPeakError extends TimingError
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
            Pair<Instant, Instant> peak = TimingErrorHelper.getTimeToPeak( next, this.getRNG() );
            
            // Duration.between is negative if the predicted/right or "end" is before the observed/left or "start"
            Duration error = Duration.between( peak.getLeft(), peak.getRight() );
            
            // Add the time-to-peak error against the basis time
            returnMe.add( Pair.of( next.getEarliestBasisTime(), error ) );
        }

        // Create output metadata with the identifier of the statistic as the component identifier
        Metadata in = s.getMetadata();
        Dimension outputDimension = MetadataFactory.getDimension( "DURATION" );
        MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( s.getBasisTimes().size(),
                                                                                             outputDimension,
                                                                                             in.getDimension(),
                                                                                             this.getID(),
                                                                                             MetricConstants.MAIN,
                                                                                             in.getIdentifier(),
                                                                                             in.getTimeWindow() );

        return DataFactory.ofPairedOutput( returnMe, meta );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.TIME_TO_PEAK_ERROR;
    }
    
    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class TimeToPeakErrorBuilder extends TimingErrorBuilder
    {

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

    protected TimeToPeakError( final TimeToPeakErrorBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }    

}
