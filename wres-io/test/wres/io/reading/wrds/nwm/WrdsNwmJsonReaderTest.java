package wres.io.reading.wrds.nwm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;

/**
 * Tests the {@link WrdsNwmJsonReader}.
 * @author James Brown
 */

class WrdsNwmJsonReaderTest
{
    private DataSource fakeSource;
    private String jsonString;

    @BeforeEach
    void setup() throws JsonProcessingException
    {
        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( null,
                                             null,
                                             null,
                                             null,
                                             null );
        this.fakeSource = DataSource.of( DataDisposition.JSON_WRDS_NWM,
                                         fakeDeclarationSource,
                                         new DataSourceConfig( null,
                                                               List.of( fakeDeclarationSource ),
                                                               null,
                                                               null,
                                                               null,
                                                               null,
                                                               null,
                                                               null,
                                                               null,
                                                               null,
                                                               null ),
                                         Collections.emptyList(),
                                         // Use a fake URI with an NWIS-like string as this is used to trigger the 
                                         // identification of an instantaneous time-scale 
                                         URI.create( "https://fake.wrds.gov/" ),
                                         LeftOrRightOrBaseline.LEFT );
        this.jsonString =
                "{\n"
                          + "  \"_documentation\": \"https://somewhere/docs/v1/nwm/swagger/\",\n"
                          + "  \"_metrics\": {\n"
                          + "    \"location_api_call\": 0.08287358283996582,\n"
                          + "    \"forming_location_data\": 0.0002918243408203125,\n"
                          + "    \"usgs_feature_id_count\": 1,\n"
                          + "    \"other_feature_id_count\": 0,\n"
                          + "    \"validate_thredds_vars\": 1.2268075942993164,\n"
                          + "    \"thredds_call\": 0.1791837215423584,\n"
                          + "    \"thredds_data_forming\": 0.0000045299530029296875,\n"
                          + "    \"response_forming\": 0.000018358230590820312,\n"
                          + "    \"total_request_time\": 1.85443115234375\n"
                          + "  },\n"
                          + "  \"_warnings\": [],\n"
                          + "  \"variable\": {\n"
                          + "    \"name\": \"streamflow\",\n"
                          + "    \"unit\": \"meter^3 / sec\"\n"
                          + "  },\n"
                          + "  \"forecasts\": [\n"
                          + "    {\n"
                          + "      \"reference_time\": \"20200112T00Z\",\n"
                          + "      \"features\": [\n"
                          + "        {\n"
                          + "          \"location\": {\n"
                          + "            \"names\": {\n"
                          + "              \"nws_lid\": \"\",\n"
                          + "              \"usgs_site_code\": \"07049000\",\n"
                          + "              \"nwm_feature_id\": \"8588002\",\n"
                          + "              \"name\": \"War Eagle Creek near Hindsville  AR\"\n"
                          + "            },\n"
                          + "            \"coordinates\": {\n"
                          + "              \"latitude\": \"36.2\",\n"
                          + "              \"longitude\": \"-93.855\"\n"
                          + "            }\n"
                          + "          },\n"
                          + "          \"members\": [\n"
                          + "            {\n"
                          + "              \"identifier\": \"1\",\n"
                          + "              \"data_points\": [\n"
                          + "                {\n"
                          + "                  \"time\": \"20200112T03Z\",\n"
                          + "                  \"value\": \"270.9899939429015\"\n"
                          + "                },\n"
                          + "                {\n"
                          + "                  \"time\": \"20200112T02Z\",\n"
                          + "                  \"value\": \"334.139992531389\"\n"
                          + "                },\n"
                          + "                {\n"
                          + "                  \"time\": \"20200112T01Z\",\n"
                          + "                  \"value\": \"382.27999145537615\"\n"
                          + "                }\n"
                          + "              ]\n"
                          + "            }\n"
                          + "          ]\n"
                          + "        }\n"
                          + "      ]\n"
                          + "    }\n"
                          + "  ]\n"
                          + "}";
    }

    @Test
    void testReadObservationsFromStreamResultsInOneTimeSeries() throws IOException
    {
        WrdsNwmJsonReader reader = WrdsNwmJsonReader.of();

        try ( InputStream inputStream = new ByteArrayInputStream( this.jsonString.getBytes() );
              Stream<TimeSeriesTuple> tupleStream = reader.read( this.fakeSource, inputStream ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                         Instant.parse( "2020-01-12T00:00:00Z" ) ),
                                                                 null,
                                                                 "streamflow",
                                                                 FeatureKey.of( MessageFactory.getGeometry( "8588002" ) ),
                                                                 "meter^3 / sec" );
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2020-01-12T01:00:00Z" ),
                                                                         382.27999145537615 ) )
                                                    .addEvent( Event.of( Instant.parse( "2020-01-12T02:00:00Z" ),
                                                                         334.139992531389 ) )
                                                    .addEvent( Event.of( Instant.parse( "2020-01-12T03:00:00Z" ),
                                                                         270.9899939429015 ) )
                                                    .setMetadata( metadata )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }
}
