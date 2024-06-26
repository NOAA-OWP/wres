package wres.io.retrieving.database;

import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeWindowOuter;

/**
 * A custom event for monitoring time-series data retrievals and exposing them to the Java Flight Recorder.
 * 
 * @author James Brown
 */

@Name( "wres.io.pooling.RetrievalEvent" )
@Label( "Retrieval Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Core", "Pooling", "Retrieval" } )
class RetrievalEvent extends Event
{
    @Label( "Orientation" )
    @Description( "The side of the pairing from which the time series data originates." )
    private final String orientation;

    @Label( "Time window" )
    @Description( "The temporal boundaries of the pool from which the time series data originates." )
    private final String timeWindow;

    @Label( "Features" )
    @Description( "The set of geographic features associated with the time series data." )
    private final String features;

    @Label( "Variable name" )
    @Description( "The name of the physical variable associated with the time series data." )
    private final String variableName;

    /**
     * @param orientation the orientation of the time-series data
     * @param timeWindow the time window, optional
     * @param features the geographic features
     * @param variableName the variable name
     * @return an instance
     * @throws NullPointerException if any required input is null
     */

    static RetrievalEvent of( DatasetOrientation orientation,
                              TimeWindowOuter timeWindow,
                              Set<Feature> features,
                              String variableName )
    {
        return new RetrievalEvent( orientation, timeWindow, features, variableName );
    }

    /**
     * Hidden constructor.
     * @param orientation the orientation of the time-series data
     * @param timeWindow the time window, optional
     * @param features the geographic features
     * @param variableName the variable name
     * @throws NullPointerException if any required input is null
     */

    private RetrievalEvent( DatasetOrientation orientation,
                            TimeWindowOuter timeWindow,
                            Set<Feature> features,
                            String variableName )
    {
        Objects.requireNonNull( orientation );
        Objects.requireNonNull( features );
        Objects.requireNonNull( variableName );

        this.orientation = orientation.toString();

        // Use the short names only
        this.features = features.stream()
                                .map( Feature::getName )
                                .collect( Collectors.toSet() )
                                .toString();
        this.variableName = variableName;

        if ( Objects.isNull( timeWindow ) )
        {
            this.timeWindow = "UNDEFINED";
        }
        else
        {
            // Short representation of the time window
            StringJoiner builder = new StringJoiner( ",", "[", "]" );
            this.timeWindow = builder.add( timeWindow.getEarliestReferenceTime().toString() )
                                     .add( timeWindow.getLatestReferenceTime().toString() )
                                     .add( timeWindow.getEarliestValidTime().toString() )
                                     .add( timeWindow.getLatestValidTime().toString() )
                                     .add( timeWindow.getEarliestLeadDuration().toString() )
                                     .add( timeWindow.getLatestLeadDuration().toString() )
                                     .toString();
        }
    }
}
