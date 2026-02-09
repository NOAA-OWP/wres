package wres.io.ingesting.database;

import java.net.URI;
import java.util.Objects;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

import wres.config.components.DatasetOrientation;
import wres.datamodel.space.Feature;

/**
 * A custom event for monitoring time-series data ingest into a database and exposing it to the Java Flight Recorder.
 *
 * @author James Brown
 */

@Name( "wres.io.ingesting.database.DatabaseIngestEvent" )
@Label( "Database Ingest Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Core", "Ingest" } )
@SuppressWarnings( { "unused", "FieldCanBeLocal" } )
public class DatabaseIngestEvent extends Event
{
    @Label( "Orientation" )
    @Description( "The side of the evaluation from which the time series data originates." )
    private final String orientation;

    @Label( "Feature name" )
    @Description( "The name of the geographic feature associated with the time series data." )
    private final String featureName;

    @Label( "Variable name" )
    @Description( "The name of the physical variable associated with the time series data." )
    private final String variableName;

    @Label( "Resource URI" )
    @Description( "The URI of the data source from which the time-series originates." )
    private final String uri;

    @Label( "Retry count" )
    @Description( "The number of retries attempted when this event was created." )
    private final int retryCount;

    /**
     * @param orientation the orientation of the time-series data
     * @param feature the geographic feature
     * @param variableName the variable name
     * @param uri the URI
     * @param retryCount the number of retries attempted when this event was registered
     * @return an instance
     * @throws NullPointerException if any required input is null
     */

    public static DatabaseIngestEvent of( DatasetOrientation orientation,
                                          Feature feature,
                                          String variableName,
                                          URI uri,
                                          int retryCount )
    {
        return new DatabaseIngestEvent( orientation, feature, variableName, uri, retryCount );
    }

    /**
     * Hidden constructor.
     * @param orientation the orientation of the time-series data
     * @param feature the geographic feature
     * @param variableName the variable name
     * @param uri the URI
     * @param retryCount the number of retries attempted when this event was registered
     * @throws NullPointerException if any required input is null
     */

    private DatabaseIngestEvent( DatasetOrientation orientation,
                                 Feature feature,
                                 String variableName,
                                 URI uri,
                                 int retryCount )
    {
        Objects.requireNonNull( orientation );
        Objects.requireNonNull( feature );
        Objects.requireNonNull( variableName );
        Objects.requireNonNull( uri );

        this.orientation = orientation.toString();
        this.featureName = feature.getName();
        this.variableName = variableName;
        this.uri = uri.toString();
        this.retryCount = retryCount;
    }
}
