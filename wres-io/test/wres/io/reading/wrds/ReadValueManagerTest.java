package wres.io.reading.wrds;

import java.io.IOException;
import java.time.Instant;
import java.net.URI;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.junit.Assert;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.MissingValues;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.config.generated.DatasourceType;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.LeftOrRightOrBaseline;

import static wres.io.reading.DataSource.DataDisposition.JSON_WRDS_AHPS;

public class ReadValueManagerTest
{

    private static final String VALID_AHPS_v2_BODY_WITH_MISSINGS =
            "{\n"
            + "    \"_documentation\": {\n"
            + "        \"swagger URL\": \"http://***REMOVED***.***REMOVED***.***REMOVED***/docs/rfc_forecast/v2.0/swagger/\"\n"
            + "    },\n"
            + "    \"deployment\": {\n"
            + "        \"api_url\": \"https://***REMOVED***.***REMOVED***.***REMOVED***/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/FROV2/\",\n"
            + "        \"stack\": \"prod\",\n"
            + "        \"version\": \"v2.4.0\",\n"
            + "        \"api_caller\": \"None\"\n"
            + "    },\n"
            + "    \"_metrics\": {\n"
            + "        \"forecast_count\": 1,\n"
            + "        \"location_count\": 1,\n"
            + "        \"total_request_time\": 1.1423799991607666\n"
            + "    },\n"
            + "    \"header\": {\n"
            + "        \"request\": {\n"
            + "            \"params\": {\n"
            + "                \"asProvided\": {},\n"
            + "                \"asUsed\": {\n"
            + "                    \"issuedTime\": \"latest\",\n"
            + "                    \"validTime\": \"all\",\n"
            + "                    \"excludePast\": \"False\",\n"
            + "                    \"minForecastStatus\": \"no_flooding\",\n"
            + "                    \"use_rating_curve\": null,\n"
            + "                    \"includeDuplicates\": \"False\",\n"
            + "                    \"returnNewestForecast\": \"False\",\n"
            + "                    \"completeForecastStatus\": \"False\"\n"
            + "                }\n"
            + "            }\n"
            + "        },\n"
            + "        \"missing_values\": [\n"
            + "            -999,\n"
            + "            -9999\n"
            + "        ]\n"
            + "    },\n"
            + "    \"forecasts\": [\n"
            + "        {\n"
            + "            \"location\": {\n"
            + "                \"names\": {\n"
            + "                    \"nwsLid\": \"FROV2\",\n"
            + "                    \"usgsSiteCode\": \"01631000\",\n"
            + "                    \"nwm_feature_id\": 5907079,\n"
            + "                    \"nwsName\": \"Front Royal\",\n"
            + "                    \"usgsName\": \"S F SHENANDOAH RIVER AT FRONT ROYAL, VA\"\n"
            + "                },\n"
            + "                \"coordinates\": {\n"
            + "                    \"latitude\": 38.91400059,\n"
            + "                    \"longitude\": -78.21083388,\n"
            + "                    \"crs\": \"NAD83\",\n"
            + "                    \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
            + "                },\n"
            + "                \"nws_coordinates\": {\n"
            + "                    \"latitude\": 38.913888888889,\n"
            + "                    \"longitude\": -78.211111111111,\n"
            + "                    \"crs\": \"NAD27\",\n"
            + "                    \"link\": \"https://maps.google.com/maps?q=38.913888888889,-78.211111111111\"\n"
            + "                },\n"
            + "                \"usgs_coordinates\": {\n"
            + "                    \"latitude\": 38.91400059,\n"
            + "                    \"longitude\": -78.21083388,\n"
            + "                    \"crs\": \"NAD83\",\n"
            + "                    \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
            + "                }\n"
            + "            },\n"
            + "            \"producer\": \"MARFC\",\n"
            + "            \"issuer\": \"LWX\",\n"
            + "            \"distributor\": \"SBN\",\n"
            + "            \"type\": \"deterministic\",\n"
            + "            \"issuedTime\": \"2021-11-14T13:46:00Z\",\n"
            + "            \"generationTime\": \"2021-11-14T13:54:18Z\",\n"
            + "            \"parameterCodes\": {\n"
            + "                \"physicalElement\": \"QR\",\n"
            + "                \"duration\": \"I\",\n"
            + "                \"typeSource\": \"FF\",\n"
            + "                \"extremum\": \"Z\",\n"
            + "                \"probability\": \"Z\"\n"
            + "            },\n"
            + "            \"thresholds\": {\n"
            + "                \"action\": null,\n"
            + "                \"minor\": 24340.0,\n"
            + "                \"moderate\": 31490.0,\n"
            + "                \"major\": 47200.0,\n"
            + "                \"record\": 130000.0\n"
            + "            },\n"
            + "            \"units\": {\n"
            + "                \"streamflow\": \"KCFS\"\n"
            + "            },\n"
            + "            \"members\": [\n"
            + "                {\n"
            + "                    \"identifier\": \"1\",\n"
            + "                    \"forecast_status\": \"no_flooding\",\n"
            + "                    \"dataPointsList\": [\n"
            + "                        [\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-14T18:00:00Z\",\n"
            + "                                \"value\": 2.12,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-15T00:00:00Z\",\n"
            + "                                \"value\": 2.12,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-15T06:00:00Z\",\n"
            + "                                \"value\": 2.01,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-15T12:00:00Z\",\n"
            + "                                \"value\": 1.89,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-15T18:00:00Z\",\n"
            + "                                \"value\": 1.78,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-16T00:00:00Z\",\n"
            + "                                \"value\": 1.67,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-16T06:00:00Z\",\n"
            + "                                \"value\": 1.67,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-16T12:00:00Z\",\n"
            + "                                \"value\": 1.52,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-16T18:00:00Z\",\n"
            + "                                \"value\": 1.52,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-17T00:00:00Z\",\n"
            + "                                \"value\":  -9999.00,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-17T06:00:00Z\",\n"
            + "                                \"value\": -999,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"time\": \"2021-11-17T12:00:00Z\",\n"
            + "                                \"value\": 1.38,\n"
            + "                                \"status\": \"no_flooding\"\n"
            + "                            }\n"
            + "                        ]\n"
            + "                    ]\n"
            + "                }\n"
            + "            ]\n"
            + "        }\n"
            + "    ]\n"
            + "}\n";

    @Test
    public void testReplaceMissingValues() throws StreamReadException, DatabindException, IOException
    {
        TimeSeriesIngester timeSeriesIngester = Mockito.mock( TimeSeriesIngester.class );
        DataSource dataSource = Mockito.mock( DataSource.class );

        ReadValueManager manager = new ReadValueManager( timeSeriesIngester,
                                                         dataSource );
        ObjectMapper mapper = new ObjectMapper();

        byte[] rawForecast = VALID_AHPS_v2_BODY_WITH_MISSINGS.getBytes();

        ForecastResponse response = mapper.readValue( rawForecast,
                                                      ForecastResponse.class );
        if ( response.forecasts == null )
        {
            Assert.fail( "Null response unexpected." );
        }
        if ( response.forecasts.length != 1 )
        {
            Assert.fail( "Expected one forecast, but found " + response.forecasts.length );
        }
        Forecast forecast = response.forecasts[0];
        TimeSeries<Double> timeSeries = manager.read( forecast, response.getHeader().getMissing_values() );

        Instant firstInstant = Instant.parse( "2021-11-17T00:00:00Z" );
        Instant secondInstant = Instant.parse( "2021-11-17T06:00:00Z" );

        for ( Event<Double> evt : timeSeries.getEvents() )
        {
            if ( firstInstant.equals( ( evt.getTime() ) ) )
            {
                Assert.assertEquals( MissingValues.DOUBLE, evt.getValue().doubleValue(), 0.0D );
            }
            if ( secondInstant.equals( ( evt.getTime() ) ) )
            {
                Assert.assertEquals( MissingValues.DOUBLE, evt.getValue().doubleValue(), 0.0D );
            }
        }
    }
    
    private static final String VALID_WRDS_OBS_BODY_WITH_MISSINGS = 
            "{\n"
            + "    \"_documentation\": {\n"
            + "        \"swaggerURL\": \"http://***REMOVED***.***REMOVED***.***REMOVED***/docs/observed/v1.0/swagger/\" \n"
            + "    },\n"
            + "    \"_deployment\": {\n"
            + "        \"apiUrl\":\"https://***REMOVED***.***REMOVED***.***REMOVED***/api/observed/v1.0/observed/streamflow/nws_lid/FROV2/?proj=WRES&validTime=%5B2022-03-01T00%3A00%3A00Z%2C2022-03-01T14%3A12%3A59Z%5D\",\n"
            + "        \"stack\": \"dev\",\n"
            + "        \"version\": \"v1.0.0\",\n"
            + "        \"apiCaller\": \"WRES\" \n"
            + "    },\n"
            + "    \"_nonUrgentIssueReportingLink\": \"https://vlab.***REMOVED***/redmine/projects/wrds-user-support/issues/new?issue[category_id]=2835\",\n"
            + "    \"_metrics\": {\n"
            + "        \"observedDataCount\": 64,\n"
            + "        \"locationCount\": 2,\n"
            + "        \"totalRequestTime\": 0.6576216220855713\n"
            + "    },\n"
            + "    \"header\": {\n"
            + "        \"request\": {\n"
            + "            \"params\": {\n"
            + "                \"asProvided\": {\n"
            + "                    \"validTime\": \"[2022-03-01T00:00:00Z,2022-03-01T14:12:59Z]\" \n"
            + "                },\n"
            + "                \"asUsed\": {\n"
            + "                    \"validTime\": \"[2022-03-01T00:00:00Z,2022-03-01T14:12:59Z]\",\n"
            + "                    \"nonUSGSGages\": false,\n"
            + "                    \"pe\": null,\n"
            + "                    \"ts\": null\n"
            + "                }\n"
            + "            }\n"
            + "        },\n"
            + "        \"missingValues\": [\n"
            + "            -999,\n"
            + "            -9999\n"
            + "        ]\n"
            + "    },\n"
            + "    \"timeseriesDataset\": [\n"
            + "        {\n"
            + "            \"location\": {\n"
            + "                \"names\": {\n"
            + "                    \"nwsLid\": \"FROV2\",\n"
            + "                    \"usgsSiteCode\": \"01631000\",\n"
            + "                    \"nwmFeatureId\": 5907079,\n"
            + "                    \"nwsName\": \"Front Royal\",\n"
            + "                    \"usgsName\": \"S F SHENANDOAH RIVER AT FRONT ROYAL, VA\" \n"
            + "                },\n"
            + "                \"nwsCoordinates\": {\n"
            + "                    \"latitude\": 38.913888888889,\n"
            + "                    \"longitude\": -78.211111111111,\n"
            + "                    \"crs\": \"NAD27\",\n"
            + "                    \"link\": \"https://maps.google.com/maps?q=38.913888888889,-78.211111111111\" \n"
            + "                },\n"
            + "                \"usgsCoordinates\": {\n"
            + "                    \"latitude\": 38.91400059,\n"
            + "                    \"longitude\": -78.21083388,\n"
            + "                    \"crs\": \"NAD83\",\n"
            + "                    \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\" \n"
            + "                }\n"
            + "            },\n"
            + "            \"producer\": \"MARFC\",\n"
            + "            \"issuer\": \"CTP\",\n"
            + "            \"distributor\": \"SBN\",\n"
            + "            \"generationTime\": \"2022-03-01T13:39:18Z\",\n"
            + "            \"parameterCodes\": {\n"
            + "                \"physicalElement\": \"QR\",\n"
            + "                \"duration\": \"I\",\n"
            + "                \"typeSource\": \"RG\" \n"
            + "            },\n"
            + "            \"thresholds\": {\n"
            + "                \"units\": \"CFS\",\n"
            + "                \"action\": null,\n"
            + "                \"minor\": 24340.0,\n"
            + "                \"moderate\": 31490.0,\n"
            + "                \"major\": 47200.0,\n"
            + "                \"record\": 130000.0\n"
            + "            },\n"
            + "            \"timeseries\": [\n"
            + "                {\n"
            + "                    \"identifier\": \"1\",\n"
            + "                    \"units\": \"KCFS\",\n"
            + "                    \"dataPoints\": [\n"
            + "                        {\n"
            + "                            \"time\": \"2022-03-01T12:30:00Z\",\n"
            + "                            \"value\": 1.62,\n"
            + "                            \"status\": \"no_flooding\" \n"
            + "                        },\n"
            + "                        {\n"
            + "                            \"time\": \"2022-03-01T12:45:00Z\",\n"
            + "                            \"value\": 1.62,\n"
            + "                            \"status\": \"no_flooding\" \n"
            + "                        },\n"
            + "                        {\n"
            + "                            \"time\": \"2022-03-01T13:00:00Z\",\n"
            + "                            \"value\": 1.62,\n"
            + "                            \"status\": \"no_flooding\" \n"
            + "                        },\n"
            + "                        {\n"
            + "                            \"time\": \"2022-03-01T13:15:00Z\",\n"
            + "                            \"value\": 1.62,\n"
            + "                            \"status\": \"no_flooding\" \n"
            + "                        },\n"
            + "                        {\n"
            + "                            \"time\": \"2022-03-01T13:30:00Z\",\n"
            + "                            \"value\": 1.62,\n"
            + "                            \"status\": \"no_flooding\" \n"
            + "                        },\n"
            + "                        {\n"
            + "                            \"time\": \"2022-03-01T13:45:00Z\",\n"
            + "                            \"value\": 1.62,\n"
            + "                            \"status\": \"no_flooding\" \n"
            + "                        },\n"
            + "                        {\n"
            + "                            \"time\": \"2022-03-01T14:00:00Z\",\n"
            + "                            \"value\": 1.65,\n"
            + "                            \"status\": \"no_flooding\" \n"
            + "                        }\n"
            + "                    ]\n"
            + "                }\n"
            + "            ]\n"
            + "        }\n"
            + "    ]\n"
            + "}\n";    


    @Test
    public void testReadingObservations() throws StreamReadException, DatabindException, IOException
    {
        URI fakeAhpsUri = URI.create( "http://localhost:8080/stuff");

        DataSourceConfig config = new DataSourceConfig( DatasourceType.OBSERVATIONS,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null );

        DataSource dataSource = DataSource.of( JSON_WRDS_AHPS,
                                               null,
                                               config, //This is needed for the check that its observations.
                                               List.of( LeftOrRightOrBaseline.LEFT,
                                                        LeftOrRightOrBaseline.RIGHT ),
                                               fakeAhpsUri,
                                               LeftOrRightOrBaseline.RIGHT );
        TimeSeriesIngester timeSeriesIngester = Mockito.mock( TimeSeriesIngester.class );

        ReadValueManager manager = new ReadValueManager( timeSeriesIngester,
                                                         dataSource );
        ObjectMapper mapper = new ObjectMapper();

        byte[] rawData = VALID_WRDS_OBS_BODY_WITH_MISSINGS.getBytes();

        ForecastResponse response = mapper.readValue( rawData,
                                                      ForecastResponse.class );
        if ( response.forecasts == null )
        {
            Assert.fail( "Null response unexpected." );
        }
        if ( response.forecasts.length != 1 )
        {
            Assert.fail( "Expected one forecast, but found " + response.forecasts.length );
        }

        //The reader is designed for forecasts, but being used for observations.  Hence the inconsistent
        //naming here where observations are set to forecasts.
        Forecast observations = response.forecasts[0];
        TimeSeries<Double> timeSeries = manager.read( observations, response.getHeader().getMissing_values() );

        Assert.assertEquals( 7, timeSeries.getEvents().size() );
        
        Instant firstInstant = Instant.parse( "2022-03-01T12:30:00Z" );
        Instant lastInstant = Instant.parse( "2022-03-01T14:00:00Z" );

        for ( Event<Double> evt : timeSeries.getEvents() )
        {
            if ( firstInstant.equals( ( evt.getTime() ) ) )
            {
                Assert.assertEquals( 1.62D, evt.getValue().doubleValue(), 0.0D );
            }
            if ( lastInstant.equals( ( evt.getTime() ) ) )
            {
                Assert.assertEquals( 1.65D, evt.getValue().doubleValue(), 0.0D );
            }
        }

    }


}

