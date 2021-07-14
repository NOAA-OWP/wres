package wres.io.pooling;

import java.util.Objects;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Threshold;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.time.TimeSeriesMetadata;

/**
 * A custom event for monitoring the rescaling of time-series data and exposing it to the Java Flight Recorder.
 * 
 * @author james.brown@hydrosolved.com
 */

@Name( "wres.io.pooling.RescalingEvent" )
@Label( "Rescaling Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Core client", "Pooling", "Rescaling" } )
@Threshold( "5000 ms" )
class RescalingEvent extends Event
{
    /**
     * Type of rescaling performed.
     */
    enum RescalingType
    {
        UPSCALED,
        DOWNSCALED;
    }

    @Label( "Type" )
    @Description( "The type of rescaling performed." )
    private final String type;

    @Label( "Orientation" )
    @Description( "The side of the pairing from which the time series data originates." )
    private final String lrb;

    @Label( "Time Series Description" )
    @Description( "The metadata of the time series that was rescaled." )
    private final String seriesMetadata;

    /**
     * @param type the type of rescaling performed
     * @param lrb the orientation of the time-series data
     * @param seriesMetadata the metadata of the rescaled time-series
     * @return an instance
     * @throws NullPointerException if any input is null
     */

    static RescalingEvent of( RescalingType type, LeftOrRightOrBaseline lrb, TimeSeriesMetadata seriesMetadata )
    {
        return new RescalingEvent( type, lrb, seriesMetadata );
    }

    /**
     * Hidden constructor.
     * @param type the type of rescaling performed
     * @param lrb the orientation of the time-series data
     * @param seriesMetadata the metadata of the rescaled time-series
     * @throws NullPointerException if any input is null
     */

    private RescalingEvent( RescalingType type, LeftOrRightOrBaseline lrb, TimeSeriesMetadata seriesMetadata )
    {
        Objects.requireNonNull( type );
        Objects.requireNonNull( lrb );
        Objects.requireNonNull( seriesMetadata );

        this.type = type.toString();
        this.lrb = lrb.toString();
        this.seriesMetadata = seriesMetadata.toString();
    }
}
