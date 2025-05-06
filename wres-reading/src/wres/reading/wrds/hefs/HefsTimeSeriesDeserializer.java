package wres.reading.wrds.hefs;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.types.Ensemble;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesHeader;

/**
 * Deserializes a collection of results from a request to the Water Resource Data Service for the Hydrologic Ensemble
 * Forecast Service.
 *
 * @author James Brown
 */
class HefsTimeSeriesDeserializer extends JsonDeserializer<List<TimeSeries<Ensemble>>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( HefsTimeSeriesDeserializer.class );

    private static final String PARAMETER_ID = "parameter_id";
    private static final String ENSEMBLE_ID = "ensemble_id";
    private static final String ENSEMBLE_MEMBER_INDEX = "ensemble_member_index";
    private static final String UNITS = "units";
    private static final String MODULE_INSTANCE_ID = "module_instance_id";
    private static final String QUALIFIER_ID = "qualifier_id";
    private static final String NULL = "null";
    private static final String LONG_NAME = "long_name";

    @Override
    public List<TimeSeries<Ensemble>> deserialize( JsonParser jsonParser,
                                                   DeserializationContext deserializationContext ) throws IOException
    {
        ObjectMapper mapper = ( ObjectMapper ) jsonParser.getCodec();
        JsonNode node = mapper.readTree( jsonParser );

        Map<TimeSeriesHeader, SortedMap<String, SortedMap<Instant, Double>>> ensembleTraces = new HashMap<>();

        int size = node.size();

        LOGGER.debug( "Detected {} traces to read, which will be read and composed into ensemble time-series", size );

        for ( int i = 0; i < size; i++ )
        {
            JsonNode nextNode = node.get( i );
            SortedMap<Instant, Double> events = this.readEvents( nextNode );

            TimeSeriesHeader metadata = this.readMetadata( nextNode );

            LOGGER.debug( "Read a time-series with the following metadata: {}.", metadata );

            SortedMap<String, SortedMap<Instant, Double>> nextTraceMap = ensembleTraces.get( metadata );

            // Create a new map
            if ( Objects.isNull( nextTraceMap ) )
            {
                nextTraceMap = new TreeMap<>();
                ensembleTraces.put( metadata, nextTraceMap );
            }

            nextTraceMap.put( metadata.ensembleMemberIndex(), events );
        }

        // Create some time-series metadata for each metadata instance and then compose the ensemble time-series
        List<TimeSeries<Ensemble>> results = new ArrayList<>();
        for ( Map.Entry<TimeSeriesHeader, SortedMap<String, SortedMap<Instant, Double>>> nextEntry : ensembleTraces.entrySet() )
        {
            TimeSeriesHeader nextMetadata = nextEntry.getKey();
            TimeSeriesMetadata timeSeriesMetadata =
                    ReaderUtilities.getTimeSeriesMetadataFromHeader( nextMetadata, ZoneOffset.UTC );
            TimeSeries<Ensemble> nextSeries =
                    ReaderUtilities.transformEnsemble( timeSeriesMetadata, nextEntry.getValue(), -1, null );
            results.add( nextSeries );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Finished reading {} time-series, which contained {} traces and each trace contained {} "
                          + "events.",
                          results.size(),
                          size,
                          results.stream().mapToLong( n -> n.getEvents().size() ).sum() );
        }

        return Collections.unmodifiableList( results );
    }

    /**
     * Reads the events from a single result node.
     *
     * @param resultNode the result node
     * @return the events
     */

    private SortedMap<Instant, Double> readEvents( JsonNode resultNode )
    {
        SortedMap<Instant, Double> events = new TreeMap<>();
        if ( resultNode.has( "events" ) )
        {
            JsonNode eventsNode = resultNode.get( "events" );
            int count = eventsNode.size();
            for ( int i = 0; i < count; i++ )
            {
                JsonNode nextEvent = eventsNode.get( i );
                if ( nextEvent.has( "date" ) && nextEvent.has( "time" ) && nextEvent.has( "value" ) )
                {
                    String date = nextEvent.get( "date" ).asText();
                    String time = nextEvent.get( "time" ).asText();
                    double value = nextEvent.get( "value" ).doubleValue();

                    Instant instant = ReaderUtilities.parseInstant( date, time, ZoneOffset.UTC );
                    events.put( instant, value );
                }
            }
        }

        return Collections.unmodifiableSortedMap( events );
    }

    /**
     * Reads the pertinent metadata from a single result node.
     *
     * @param resultNode the result node
     * @return the metadata
     */

    private TimeSeriesHeader readMetadata( JsonNode resultNode )
    {
        TimeSeriesHeader.TimeSeriesHeaderBuilder builder = this.readGeospatialMetadata( resultNode ).toBuilder();

        if ( resultNode.has( PARAMETER_ID ) )
        {
            String parameterId = resultNode.get( PARAMETER_ID ).asText();
            builder.parameterId( parameterId );
        }

        if ( resultNode.has( ENSEMBLE_ID ) )
        {
            String ensembleId = resultNode.get( ENSEMBLE_ID ).asText();
            builder.ensembleId( ensembleId );
        }

        if ( resultNode.has( ENSEMBLE_MEMBER_INDEX ) )
        {
            String ensembleMemberIndex = resultNode.get( ENSEMBLE_MEMBER_INDEX )
                                                   .asText();
            builder.ensembleMemberIndex( ensembleMemberIndex );
        }

        if ( resultNode.has( "forecast_date_date" ) )
        {
            String forecastDateDate = resultNode.get( "forecast_date_date" )
                                                .asText();
            builder.forecastDateDate( forecastDateDate );
        }

        if ( resultNode.has( "forecast_date_time" ) )
        {
            String forecastDateTime = resultNode.get( "forecast_date_time" )
                                                .asText();
            builder.forecastDateTime( forecastDateTime );
        }

        if ( resultNode.has( UNITS ) )
        {
            String units = resultNode.get( UNITS )
                                     .asText();
            builder.units( units );
        }

        if ( resultNode.has( MODULE_INSTANCE_ID )
             && !NULL.equals( resultNode.get( MODULE_INSTANCE_ID )
                                        .asText() ) )
        {
            String moduleInstanceId = resultNode.get( MODULE_INSTANCE_ID )
                                                .asText();
            builder.moduleInstanceId( moduleInstanceId );
        }

        if ( resultNode.has( QUALIFIER_ID )
             && !NULL.equals( resultNode.get( QUALIFIER_ID )
                                        .asText() ) )
        {
            String qualifierId = resultNode.get( QUALIFIER_ID )
                                           .asText();
            builder.qualifierId( qualifierId );
        }

        if ( resultNode.has( "type" ) )
        {
            String type = resultNode.get( "type" )
                                    .asText();
            builder.type( type );
        }

        if ( resultNode.has( "time_step_multiplier" ) )
        {
            String timeStepMultiplier = resultNode.get( "time_step_multiplier" )
                                                  .asText();
            builder.timeStepMultiplier( timeStepMultiplier );
        }

        if ( resultNode.has( "time_step_unit" ) )
        {
            String timeStepUnit = resultNode.get( "time_step_unit" )
                                            .asText();
            builder.timeStepUnit( timeStepUnit );
        }

        return builder.build();
    }

    /**
     * Reads the pertinent geospatial metadata from a single result node. Adding or returning a builder fails the
     * javadoc build, which is a known issue with lombok/javadoc that is apparently fixed by "delomboking" the source,
     * which sounds like an anti-pattern, so not doing it.
     *
     * @param resultNode the result node
     * @return the geospatial metadata
     */

    private TimeSeriesHeader readGeospatialMetadata( JsonNode resultNode )
    {
        TimeSeriesHeader.TimeSeriesHeaderBuilder builder = TimeSeriesHeader.builder();

        if ( resultNode.has( "location_id" ) )
        {
            String locationId = resultNode.get( "location_id" ) // NOSONAR
                                          .asText();
            builder.locationId( locationId );
        }

        if ( resultNode.has( LONG_NAME ) && !NULL.equals( resultNode.get( LONG_NAME ).asText() ) )
        {
            String locationDescription = resultNode.get( LONG_NAME ) // NOSONAR
                                                   .asText();
            builder.locationDescription( locationDescription );
        }

        if ( resultNode.has( "x" ) )
        {
            String x = resultNode.get( "x" ) // NOSONAR
                                 .asText();
            builder.x( x );
        }

        if ( resultNode.has( "y" ) )
        {
            String y = resultNode.get( "y" ) // NOSONAR
                                 .asText();
            builder.y( y );
        }

        if ( resultNode.has( "z" ) )
        {
            String z = resultNode.get( "z" ) // NOSONAR
                                 .asText();
            builder.z( z );
        }

        if ( resultNode.has( "lat" ) )
        {
            String latitude = resultNode.get( "lat" ) // NOSONAR
                                        .asText();
            builder.latitude( latitude );
        }

        if ( resultNode.has( "lon" ) )
        {
            String longitude = resultNode.get( "lon" ) // NOSONAR
                                         .asText();
            builder.longitude( longitude );
        }

        return builder.build();
    }

}