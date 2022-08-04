package wres.io.reading;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.ingesting.PreIngestException;
import wres.io.utilities.WebClient;

/**
 * Utilities for file reading.
 * 
 * @author James Brown
 */

public class ReaderUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ReaderUtilities.class );

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient();

    /**
     * Transform a single trace into a {@link TimeSeries} of {@link Double} values.
     * @param metadata The metadata of the timeseries.
     * @param trace The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @return The complete TimeSeries
     */

    public static TimeSeries<Double> transform( TimeSeriesMetadata metadata,
                                                SortedMap<Instant, Double> trace,
                                                int lineNumber )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( trace );

        if ( trace.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot transform fewer than "
                                                + "one values into timeseries "
                                                + "with metadata "
                                                + metadata
                                                + " from line number "
                                                + lineNumber );
        }

        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, Double> events : trace.entrySet() )
        {
            Event<Double> event = Event.of( events.getKey(), events.getValue() );
            builder.addEvent( event );
        }

        return builder.build();
    }

    /**
     * Transform a map of traces into a {@link TimeSeries} of {@link Ensemble} values (flip it) but
     * also validate the density and valid datetimes of the ensemble prior.
     * @param metadata The metadata of the timeseries.
     * @param traces The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @param uri a uri to help with messaging
     * @return The complete TimeSeries
     * @throws IllegalArgumentException When fewer than two traces given.
     * @throws PreIngestException When ragged (non-dense) data given.
     */

    public static TimeSeries<Ensemble> transformEnsemble( TimeSeriesMetadata metadata,
                                                          SortedMap<String, SortedMap<Instant, Double>> traces,
                                                          int lineNumber,
                                                          URI uri )
    {
        int traceCount = traces.size();

        // See #62993-4: be lenient.
        if ( traceCount < 2 )
        {
            LOGGER.debug( "Found 'ensemble' data with fewer than two traces: {}",
                          traces );
        }

        Map<Instant, double[]> reshapedValues = new HashMap<>();
        Map.Entry<String, SortedMap<Instant, Double>> previousTrace = null;
        int i = 0;

        for ( Map.Entry<String, SortedMap<Instant, Double>> trace : traces.entrySet() )
        {
            SortedSet<Instant> theseInstants = new TreeSet<>( trace.getValue()
                                                                   .keySet() );

            if ( Objects.nonNull( previousTrace ) )
            {
                SortedSet<Instant> previousInstants = new TreeSet<>( previousTrace.getValue()
                                                                                  .keySet() );
                if ( !theseInstants.equals( previousInstants ) )
                {
                    throw new ReadException( "Cannot build ensemble from "
                                             + uri
                                             + " with data at or before "
                                             + "line number "
                                             + lineNumber
                                             + " because the trace named "
                                             + trace.getKey()
                                             + " had these valid datetimes"
                                             + ": "
                                             + theseInstants
                                             + " but previous trace named "
                                             + previousTrace.getKey()
                                             + " had different ones: "
                                             + previousInstants
                                             + " which is not allowed. All"
                                             + " traces must be dense and "
                                             + "match valid datetimes." );
                }
            }

            for ( Map.Entry<Instant, Double> event : trace.getValue()
                                                          .entrySet() )
            {
                Instant validDateTime = event.getKey();

                if ( !reshapedValues.containsKey( validDateTime ) )
                {
                    reshapedValues.put( validDateTime, new double[traceCount] );
                }

                double[] values = reshapedValues.get( validDateTime );
                values[i] = event.getValue();
            }

            previousTrace = trace;
            i++;
        }

        wres.datamodel.time.TimeSeries.Builder<Ensemble> builder =
                new wres.datamodel.time.TimeSeries.Builder<>();

        // Because the iteration is over a sorted map, assuming same order here.
        SortedSet<String> traceNamesSorted = new TreeSet<>( traces.keySet() );
        String[] traceNames = new String[traceNamesSorted.size()];
        traceNamesSorted.toArray( traceNames );
        Labels labels = Labels.of( traceNames );

        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, double[]> events : reshapedValues.entrySet() )
        {
            Ensemble ensembleSlice = Ensemble.of( events.getValue(), labels );
            Event<Ensemble> ensembleEvent = Event.of( events.getKey(), ensembleSlice );
            builder.addEvent( ensembleEvent );
        }

        return builder.build();
    }

    /**
     * A helper that tries to guess the time-scale information from the composition of the supplied URI. This works 
     * when particular time-series data services use fixed time-scales, such as the USGS NWIS Instantaneous Values 
     * Service.
     * 
     * @param uri the uri
     * @return the time scale or null
     */

    public static TimeScaleOuter getTimeScaleFromUri( URI uri )
    {

        TimeScaleOuter returnMe = null;

        // Assume that USGS "IV" service implies "instantaneous" values
        if ( Objects.nonNull( uri ) && uri.toString()
                                          .contains( "usgs.gov/nwis/iv" ) )
        {
            returnMe = TimeScaleOuter.of();
        }

        LOGGER.debug( "Identified {} as a source of time-series data whose time-scale is always {}.", uri, returnMe );

        return returnMe;
    }

    /**
     * Returns a byte stream from a file or web source.
     * 
     * @param uri the uri
     * @return the byte stream
     * @throws UnsupportedOperationException if the uri scheme is not one of http(s) or file
     * @throws ReadException if the stream could not be created for any other reason
     */

    public static InputStream getByteStreamFromUri( URI uri )
    {
        Objects.requireNonNull( uri );
        try
        {
            if ( uri.getScheme().equals( "file" ) )
            {
                LOGGER.debug( "Discovered a file URI scheme from which to return a stream: {}.", uri );
                Path path = Paths.get( uri );
                return new BufferedInputStream( Files.newInputStream( path ) );
            }
            else if ( uri.getScheme().toLowerCase().startsWith( "http" ) )
            {
                try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri ) )
                {
                    int httpStatus = response.getStatusCode();

                    if ( httpStatus == 404 )
                    {
                        LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                                     httpStatus,
                                     uri );
                        
                        return null;
                    }
                    else if ( ! ( httpStatus >= 200 && httpStatus < 300 ) )
                    {
                        throw new ReadException( "Failed to read data from '"
                                                 + uri
                                                 +
                                                 "' due to HTTP status code "
                                                 + httpStatus );
                    }

                    if ( LOGGER.isTraceEnabled() )
                    {
                        byte[] rawData = IOUtils.toByteArray( response.getResponse() );

                        LOGGER.trace( "Response body for {}: {}",
                                      uri,
                                      new String( rawData,
                                                  StandardCharsets.UTF_8 ) );
                    }

                    return response.getResponse();
                }
            }
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to acquire a byte stream from "
                                     + uri
                                     + ".",
                                     e );
        }

        throw new UnsupportedOperationException( "Only file and http(s) are supported. Got: "
                                                 + uri );
    }

    /**
     * Do not construct.
     */

    private ReaderUtilities()
    {
    }
}
