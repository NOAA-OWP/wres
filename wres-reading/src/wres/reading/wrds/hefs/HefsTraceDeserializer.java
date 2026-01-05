package wres.reading.wrds.hefs;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesHeader;

/**
 * <p>Deserializes a single trace from an ensemble forecast contained in a response from the Water Resources Data Service
 * (WRDS) for the Hydrologic Ensemble Forecast Service. An example document:
 * <p>
 * [
 *   [
 *     {
 *       "creation_datetime": "2025-10-05T14:51:00Z",
 *       "end_datetime": "2025-11-04T12:00:00Z",
 *       "ensemble_id": "MEFP",
 *       "ensemble_member_index": 1992,
 *       "forecast_datetime": "2025-10-05T12:00:00Z",
 *       "lat": 32.02,
 *       "location_id": "RDBN5",
 *       "lon": -104.05,
 *       "parameter_id": "QINE",
 *       "start_datetime": "2025-10-05T12:00:00Z",
 *       "station_name": "RDBN5 - Red Bluff NM - Delaware River",
 *       "time_step_multiplier": "21600",
 *       "time_step_unit": "second",
 *       "type": "instantaneous",
 *       "units": "CFS",
 *       "x": -104.05,
 *       "y": 32.02,
 *       "z": 883.92,
 *       "events": [
 *         {
 *           "flag": "2",
 *           "value": 0.7000038,
 *           "valid_datetime": "2025-10-05T12:00:00Z"
 *         },
 *         {
 *           "flag": "2",
 *           "value": 0.7000038,
 *           "valid_datetime": "2025-10-05T18:00:00Z"
 *         }
 *       ]
 *     },
 *     {
 *       "creation_datetime": "2025-10-05T14:51:00Z",
 *       "end_datetime": "2025-11-04T12:00:00Z",
 *       "ensemble_id": "MEFP",
 *       "ensemble_member_index": 1993,
 *       "forecast_datetime": "2025-10-05T12:00:00Z",
 *       "lat": 32.02,
 *       "location_id": "RDBN5",
 *       "lon": -104.05,
 *       "parameter_id": "QINE",
 *       "start_datetime": "2025-10-05T12:00:00Z",
 *       "station_name": "RDBN5 - Red Bluff NM - Delaware River",
 *       "time_step_multiplier": "21600",
 *       "time_step_unit": "second",
 *       "type": "instantaneous",
 *       "units": "CFS",
 *       "x": -104.05,
 *       "y": 32.02,
 *       "z": 883.92,
 *       "events": [
 *         {
 *           "flag": "2",
 *           "value": 0.7000038,
 *           "valid_datetime": "2025-10-05T12:00:00Z"
 *         },
 *         {
 *           "flag": "2",
 *           "value": 0.7000038,
 *           "valid_datetime": "2025-10-05T18:00:00Z"
 *         }
 *       ]
 *     },
 *   ]
 * ]
 *
 * @author James Brown
 */
class HefsTraceDeserializer extends ValueDeserializer<HefsTrace>
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
    private static final String STATION_NAME = "station_name";

    @Override
    public HefsTrace deserialize( JsonParser jsonParser,
                                  DeserializationContext deserializationContext )
    {
        ObjectReadContext mapper = jsonParser.objectReadContext();
        JsonNode node = mapper.readTree( jsonParser );

        TimeSeriesHeader header = this.readHeader( node );
        LOGGER.debug( "Read a time-series with the following metadata: {}.", header );

        SortedSet<Event<Double>> events = this.readEvents( node );

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
                if ( nextEvent.has( "valid_datetime" )
                     && nextEvent.has( "value" ) )
                {
                    String validDateTime = nextEvent.get( "valid_datetime" )
                                                    .asString();
                    double value = nextEvent.get( "value" )
                                            .doubleValue();

                    Instant instant = Instant.parse( validDateTime );
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
        TimeSeriesHeader.TimeSeriesHeaderBuilder builder = this.readGeospatialMetadata( resultNode )
                                                               .toBuilder();

        if ( resultNode.has( PARAMETER_ID ) )
        {
            String parameterId = resultNode.get( PARAMETER_ID ).asString();
            builder.parameterId( parameterId );
            LOGGER.debug( "Discovered a parameter_id of {}.", parameterId );
        }

        if ( resultNode.has( ENSEMBLE_ID ) )
        {
            String ensembleId = resultNode.get( ENSEMBLE_ID ).asString();
            builder.ensembleId( ensembleId );
            LOGGER.debug( "Discovered an ensemble_id of {}.", ensembleId );
        }

        if ( resultNode.has( ENSEMBLE_MEMBER_INDEX ) )
        {
            String ensembleMemberIndex = resultNode.get( ENSEMBLE_MEMBER_INDEX )
                                                   .asString();
            builder.ensembleMemberIndex( ensembleMemberIndex );
            LOGGER.debug( "Discovered an ensemble_member_index of {}.", ensembleMemberIndex );
        }

        if ( resultNode.has( "forecast_datetime" ) )
        {
            String forecastDateTime = resultNode.get( "forecast_datetime" )
                                                .asString();
            LOGGER.debug( "Discovered a forecast_datetime of {}.", forecastDateTime );
            String[] split = forecastDateTime.split( "T" );
            builder.forecastDateDate( split[0] );
            builder.forecastDateTime( split[1].replace( "Z", "" ) );
        }

        if ( resultNode.has( UNITS ) )
        {
            String units = resultNode.get( UNITS )
                                     .asString();
            builder.units( units );
            LOGGER.debug( "Discovered units of {}.", units );
        }

        if ( resultNode.has( MODULE_INSTANCE_ID )
             && !NULL.equals( resultNode.get( MODULE_INSTANCE_ID )
                                        .asString() ) )
        {
            String moduleInstanceId = resultNode.get( MODULE_INSTANCE_ID )
                                                .asString();
            builder.moduleInstanceId( moduleInstanceId );
            LOGGER.debug( "Discovered a module_instance_id of {}.", moduleInstanceId );
        }

        if ( resultNode.has( QUALIFIER_ID )
             && !NULL.equals( resultNode.get( QUALIFIER_ID )
                                        .asString() ) )
        {
            String qualifierId = resultNode.get( QUALIFIER_ID )
                                           .asString();
            builder.qualifierId( qualifierId );
            LOGGER.debug( "Discovered a qualifier_id of {}.", qualifierId );
        }

        if ( resultNode.has( "type" ) )
        {
            String type = resultNode.get( "type" )
                                    .asString();
            builder.type( type );
            LOGGER.debug( "Discovered a type of {}.", type );
        }

        if ( resultNode.has( "time_step_multiplier" ) )
        {
            String timeStepMultiplier = resultNode.get( "time_step_multiplier" )
                                                  .asString();
            builder.timeStepMultiplier( timeStepMultiplier );
            LOGGER.debug( "Discovered a time_step_multiplier of {}.", timeStepMultiplier );
        }

        if ( resultNode.has( "time_step_unit" ) )
        {
            String timeStepUnit = resultNode.get( "time_step_unit" )
                                            .asString();
            builder.timeStepUnit( timeStepUnit );
            LOGGER.debug( "Discovered a time_step_unit of {}.", timeStepUnit );
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
                                          .asString();
            builder.locationId( locationId );
        }

        if ( resultNode.has( STATION_NAME )
             && !NULL.equals( resultNode.get( STATION_NAME )
                                        .asString() ) )
        {
            String locationDescription = resultNode.get( STATION_NAME ) // NOSONAR
                                                   .asString();
            builder.locationDescription( locationDescription );
        }

        if ( resultNode.has( "x" ) )
        {
            String x = resultNode.get( "x" ) // NOSONAR
                                 .asString();
            builder.x( x );
        }

        if ( resultNode.has( "y" ) )
        {
            String y = resultNode.get( "y" ) // NOSONAR
                                 .asString();
            builder.y( y );
        }

        if ( resultNode.has( "z" ) )
        {
            String z = resultNode.get( "z" ) // NOSONAR
                                 .asString();
            builder.z( z );
        }

        if ( resultNode.has( "lat" ) )
        {
            String latitude = resultNode.get( "lat" ) // NOSONAR
                                        .asString();
            builder.latitude( latitude );
        }

        if ( resultNode.has( "lon" ) )
        {
            String longitude = resultNode.get( "lon" ) // NOSONAR
                                         .asString();
            builder.longitude( longitude );
        }

        return builder.build();
    }

}