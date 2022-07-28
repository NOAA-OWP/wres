package wres.io.reading.waterml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets ;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.TimeSeries;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.Source;
import wres.io.utilities.WebClient;
import wres.io.utilities.Database;

/**
 * Adapter from BasicSource to WaterMLSource (to fit pattern in ReaderFactory).
 */

public class WaterMLBasicSource implements Source
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WaterMLBasicSource.class );
    private static final WebClient WEB_CLIENT = new WebClient();
    static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    private final TimeSeriesIngester timeSeriesIngester;
    private final DataSource dataSource;

    public WaterMLBasicSource( TimeSeriesIngester timeSeriesIngester,
                               DataSource dataSource )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( dataSource );
        
        this.timeSeriesIngester = timeSeriesIngester;
        this.dataSource = dataSource;
    }

    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }
    
    @Override
    public List<IngestResult> save()
    {
        try
        {
            return this.ingest();
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a Water ML source.", e );
        }
    }

    private Pair<Response, SourceDetails> deserializeInput(URI location) throws IOException {
        try {
            if (location.getScheme().equals("file")) {
                try (InputStream data = this.getFromFile(location)) {
                    byte[] rawForecast = IOUtils.toByteArray(data);
                    return Pair.of(OBJECT_MAPPER.readValue(rawForecast, Response.class), null);
                }
            } else if (location.getScheme().toLowerCase().startsWith("http")) {
                try (WebClient.ClientResponse response = WEB_CLIENT.getFromWeb(location)) {
                    int httpStatus = response.getStatusCode();

                    if (httpStatus == 404) {
                        LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                                     httpStatus,
                                     location );
                        return Pair.of(null, null );
                    } else if (!(httpStatus >= 200 && httpStatus < 300)) {
                        throw new PreIngestException("Failed to get data from '"
                                + location +
                                "' due to HTTP status code "
                                + httpStatus);
                    }

                    byte[] rawForecast = IOUtils.toByteArray(response.getResponse());

                    if ( LOGGER.isTraceEnabled() )
                    {
                        LOGGER.trace( "Response body for {}: {}",
                                location,
                                new String( rawForecast,
                                        StandardCharsets.UTF_8 ) );
                    }

                    return Pair.of(OBJECT_MAPPER.readValue(rawForecast, Response.class), null);
                }
            }
        }
        catch ( JsonMappingException jme )
        {
            throw new PreIngestException( "Failed to parse the response body"
                    + " from USGS url "
                    + location,
                    jme );
        }

        throw new UnsupportedOperationException("Only file and http(s) "
                + "are supported. Got: "
                + location);
    }

    private List<IngestResult> ingest() throws IOException
    {
        URI location = this.getDataSource()
                           .getUri();

        Pair<Response, SourceDetails> responsePair = this.deserializeInput(location);

        if (responsePair.getLeft() == null) {
            return Collections.emptyList();
        }

        Response response = responsePair.getLeft();

        TimeSeriesIngester ingester = this.getTimeSeriesIngester();
        
        try
        {
            WaterMLSource waterMLSource = new WaterMLSource( this.getDataSource(), response );

            List<TimeSeries<Double>> transformed = waterMLSource.call();
            List<IngestResult> ingestResults = new ArrayList<>( transformed.size() );

            for ( TimeSeries<Double> timeSeries : transformed )
            {
                List<IngestResult> result = ingester.ingestSingleValuedTimeSeries( timeSeries, this.getDataSource() );
                ingestResults.addAll( result );
            }

            if ( LOGGER.isDebugEnabled() )
            {
                long countIngested = ingestResults.stream()
                                                  .filter( f -> !f.requiresRetry() )
                                                  .count();
                LOGGER.debug( "{} USGS time series ingested from URL {}",
                              countIngested, this.getDataSource()
                                                 .getUri() );
            }

            return Collections.unmodifiableList( ingestResults );
        }
        catch ( IngestException e )
        {
            throw new IngestException( "Values from USGS url "
                                       + location
                                       + " could not be ingested.",
                                       e );
        }
    }

    private InputStream getFromFile( URI uri ) throws FileNotFoundException
    {
        if ( !uri.getScheme().equals( "file" ) )
        {
            throw new IllegalArgumentException(
                    "Must pass a file uri, got " + uri );
        }

        Path forecastPath = Paths.get( uri );
        File forecastFile = forecastPath.toFile();
        return new FileInputStream( forecastFile );
    }

    /**
     * @return the data source
     */
    private DataSource getDataSource()
    {
        return this.dataSource;
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceKey the first arg to SourceDetails
     * @return a SourceDetails
     */

    SourceDetails createSourceDetails( String sourceKey )
    {
        return new SourceDetails( sourceKey );
    }


    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceDetails the first arg to SourceCompletedDetails
     * @return a SourceCompleter
     */
    SourceCompletedDetails createSourceCompletedDetails( Database database,
                                                         SourceDetails sourceDetails )
    {
        return new SourceCompletedDetails( database, sourceDetails );
    }
}
