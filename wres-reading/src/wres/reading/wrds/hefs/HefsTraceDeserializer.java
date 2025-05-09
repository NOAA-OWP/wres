package wres.reading.wrds.hefs;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesHeader;

/**
 * Deserializes a single trace from an ensemble forecast contained in a response from the Water Resources Data Service
 * (WRDS) for the Hydrologic Ensemble Forecast Service.
 *
 * @author James Brown
 */
class HefsTraceDeserializer extends JsonDeserializer<HefsTrace>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( HefsTraceDeserializer.class );

    private static final String PARAMETER_ID = "parameter_id";
    private static final String ENSEMBLE_ID = "ensemble_id";
    private static final String ENSEMBLE_MEMBER_INDEX = "ensemble_member_index";
    private static final String UNITS = "units";
    private static final String MODULE_INSTANCE_ID = "module_instance_id";
    private static final String QUALIFIER_ID = "qualifier_id";
    private static final String NULL = "null";
    private static final String LONG_NAME = "long_name";

    @Override
    public HefsTrace deserialize( JsonParser jsonParser,
                                  DeserializationContext deserializationContext ) throws IOException
    {
        ObjectMapper mapper = ( ObjectMapper ) jsonParser.getCodec();
        JsonNode node = mapper.readTree( jsonParser );

        SortedSet<Event<Double>> events = this.readEvents( node );
        TimeSeriesHeader header = this.readHeader( node );

        LOGGER.debug( "Read a time-series with the following header: {}.", header );

        TimeSeriesMetadata metadata = ReaderUtilities.getTimeSeriesMetadataFromHeader( header, ZoneOffset.UTC );
        TimeSeries<Double> timeSeries = TimeSeries.of( metadata, events );

        return new HefsTrace( header, timeSeries );
    }

    /**
     * Reads the events from a single result node.
     *
     * @param resultNode the result node
     * @return the events
     */

    private SortedSet<Event<Double>> readEvents( JsonNode resultNode )
    {
        SortedSet<Event<Double>> events = new TreeSet<>();
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
                    Event<Double> event = Event.of( instant, value );
                    events.add( event );
                }
            }
        }

        return Collections.unmodifiableSortedSet( events );
    }

    /**
     * Reads the pertinent metadata from a single result node.
     *
     * @param resultNode the result node
     * @return the metadata
     */

    private TimeSeriesHeader readHeader( JsonNode resultNode )
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