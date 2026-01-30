package wres.io.ingesting;

import java.net.URI;
import java.util.Objects;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

/**
 * A custom event for monitoring the loading of a data source (reading and ingest) and exposing this to the Java Flight
 * Recorder.
 *
 * @author James Brown
 */

@Name( "wres.io.ingesting.SourceLoadEvent" )
@Label( "Source Load Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Core", "Reading" } )
@SuppressWarnings( { "unused", "FieldCanBeLocal" } )
public class SourceLoadEvent extends Event
{
    @Label( "Resource URI" )
    @Description( "The URI of the data source that was loaded." )
    private final String uri;

    /**
     * @param uri the URI of the data source
     * @return an instance
     * @throws NullPointerException if the uri is null
     */

    public static SourceLoadEvent of( URI uri )
    {
        return new SourceLoadEvent( uri );
    }

    /**
     * Hidden constructor.
     * @param uri the URI of the data source
     * @throws NullPointerException if the uri is null
     */

    private SourceLoadEvent( URI uri )
    {
        Objects.requireNonNull( uri );

        this.uri = uri.toString();
    }
}