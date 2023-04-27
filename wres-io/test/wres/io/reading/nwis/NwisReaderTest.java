package wres.io.reading.nwis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;
import org.mockserver.model.Parameters;
import org.mockserver.verify.VerificationTimes;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.config.generated.DateCondition;
import wres.config.generated.NamedFeature;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.statistics.MessageFactory;
import wres.system.SystemSettings;

/**
 * Tests the {@link NwisReader}.
 * @author James Brown
 */

class NwisReaderTest
{
    /** Mocker server instance. */
    private ClientAndServer mockServer;

    /** Path used by GET. */
    private static final String PATH = "/nwis/iv/";

    /** Parameters used by GET. */
    private static final String PARAMS =
            "?format=json&indent=on&sites=09165000&parameterCd=00060&startDT=2018-10-01T00:00:00Z&endDT=2018-10-01T01:00:00Z";

    /** Response from GET. */
    private static final String RESPONSE = "{\"name\" : \"ns1:timeSeriesResponseType\",\r\n"
                                           + "\"declaredType\" : \"org.cuahsi.waterml.TimeSeriesResponseType\",\r\n"
                                           + "\"scope\" : \"javax.xml.bind.JAXBElement$GlobalScope\",\r\n"
                                           + "\"value\" : {\r\n"
                                           + "  \"queryInfo\" : {\r\n"
                                           + "    \"queryURL\" : \"http://nwis.waterservices.usgs.gov/nwis/iv/format=json&indent=on&sites=09165000&parameterCd=00060&startDT=2018-10-01T00:00:00Z&endDT=2018-10-01T01:00:00Z\",\r\n"
                                           + "    \"criteria\" : {\r\n"
                                           + "      \"locationParam\" : \"[ALL:09165000]\",\r\n"
                                           + "      \"variableParam\" : \"[00060]\",\r\n"
                                           + "      \"timeParam\" : {\r\n"
                                           + "        \"beginDateTime\" : \"2018-10-01T00:00:00.000\",\r\n"
                                           + "        \"endDateTime\" : \"2018-10-01T01:00:00.000\"\r\n"
                                           + "      },\r\n"
                                           + "      \"parameter\" : [ ]\r\n"
                                           + "    },\r\n"
                                           + "    \"note\" : [ {\r\n"
                                           + "      \"value\" : \"[ALL:09165000]\",\r\n"
                                           + "      \"title\" : \"filter:sites\"\r\n"
                                           + "    }, {\r\n"
                                           + "      \"value\" : \"[mode=RANGE, modifiedSince=null] interval={INTERVAL[2018-10-01T00:00:00.000Z/2018-10-01T01:00:00.000Z]}\",\r\n"
                                           + "      \"title\" : \"filter:timeRange\"\r\n"
                                           + "    }, {\r\n"
                                           + "      \"value\" : \"methodIds=[ALL]\",\r\n"
                                           + "      \"title\" : \"filter:methodId\"\r\n"
                                           + "    }, {\r\n"
                                           + "      \"value\" : \"2022-08-08T21:34:38.616Z\",\r\n"
                                           + "      \"title\" : \"requestDT\"\r\n"
                                           + "    }, {\r\n"
                                           + "      \"value\" : \"e7c64270-1761-11ed-911a-4cd98f86fad9\",\r\n"
                                           + "      \"title\" : \"requestId\"\r\n"
                                           + "    }, {\r\n"
                                           + "      \"value\" : \"Provisional data are subject to revision. Go to http://waterdata.usgs.gov/nwis/help/?provisional for more information.\",\r\n"
                                           + "      \"title\" : \"disclaimer\"\r\n"
                                           + "    }, {\r\n"
                                           + "      \"value\" : \"nadww02\",\r\n"
                                           + "      \"title\" : \"server\"\r\n"
                                           + "    } ]\r\n"
                                           + "  },\r\n"
                                           + "  \"timeSeries\" : [ {\r\n"
                                           + "    \"sourceInfo\" : {\r\n"
                                           + "      \"siteName\" : \"DOLORES RIVER BELOW RICO, CO.\",\r\n"
                                           + "      \"siteCode\" : [ {\r\n"
                                           + "        \"value\" : \"09165000\",\r\n"
                                           + "        \"network\" : \"NWIS\",\r\n"
                                           + "        \"agencyCode\" : \"USGS\"\r\n"
                                           + "      } ],\r\n"
                                           + "      \"timeZoneInfo\" : {\r\n"
                                           + "        \"defaultTimeZone\" : {\r\n"
                                           + "          \"zoneOffset\" : \"-07:00\",\r\n"
                                           + "          \"zoneAbbreviation\" : \"MST\"\r\n"
                                           + "        },\r\n"
                                           + "        \"daylightSavingsTimeZone\" : {\r\n"
                                           + "          \"zoneOffset\" : \"-06:00\",\r\n"
                                           + "          \"zoneAbbreviation\" : \"MDT\"\r\n"
                                           + "        },\r\n"
                                           + "        \"siteUsesDaylightSavingsTime\" : true\r\n"
                                           + "      },\r\n"
                                           + "      \"geoLocation\" : {\r\n"
                                           + "        \"geogLocation\" : {\r\n"
                                           + "          \"srs\" : \"EPSG:4326\",\r\n"
                                           + "          \"latitude\" : 37.63888428,\r\n"
                                           + "          \"longitude\" : -108.0603517\r\n"
                                           + "        },\r\n"
                                           + "        \"localSiteXY\" : [ ]\r\n"
                                           + "      },\r\n"
                                           + "      \"note\" : [ ],\r\n"
                                           + "      \"siteType\" : [ ],\r\n"
                                           + "      \"siteProperty\" : [ {\r\n"
                                           + "        \"value\" : \"ST\",\r\n"
                                           + "        \"name\" : \"siteTypeCd\"\r\n"
                                           + "      }, {\r\n"
                                           + "        \"value\" : \"14030002\",\r\n"
                                           + "        \"name\" : \"hucCd\"\r\n"
                                           + "      }, {\r\n"
                                           + "        \"value\" : \"08\",\r\n"
                                           + "        \"name\" : \"stateCd\"\r\n"
                                           + "      }, {\r\n"
                                           + "        \"value\" : \"08033\",\r\n"
                                           + "        \"name\" : \"countyCd\"\r\n"
                                           + "      } ]\r\n"
                                           + "    },\r\n"
                                           + "    \"variable\" : {\r\n"
                                           + "      \"variableCode\" : [ {\r\n"
                                           + "        \"value\" : \"00060\",\r\n"
                                           + "        \"network\" : \"NWIS\",\r\n"
                                           + "        \"vocabulary\" : \"NWIS:UnitValues\",\r\n"
                                           + "        \"variableID\" : 45807197,\r\n"
                                           + "        \"default\" : true\r\n"
                                           + "      } ],\r\n"
                                           + "      \"variableName\" : \"Streamflow, ft&#179;/s\",\r\n"
                                           + "      \"variableDescription\" : \"Discharge, cubic feet per second\",\r\n"
                                           + "      \"valueType\" : \"Derived Value\",\r\n"
                                           + "      \"unit\" : {\r\n"
                                           + "        \"unitCode\" : \"ft3/s\"\r\n"
                                           + "      },\r\n"
                                           + "      \"options\" : {\r\n"
                                           + "        \"option\" : [ {\r\n"
                                           + "          \"name\" : \"Statistic\",\r\n"
                                           + "          \"optionCode\" : \"00000\"\r\n"
                                           + "        } ]\r\n"
                                           + "      },\r\n"
                                           + "      \"note\" : [ ],\r\n"
                                           + "      \"noDataValue\" : -999999.0,\r\n"
                                           + "      \"variableProperty\" : [ ],\r\n"
                                           + "      \"oid\" : \"45807197\"\r\n"
                                           + "    },\r\n"
                                           + "    \"values\" : [ {\r\n"
                                           + "      \"value\" : [ {\r\n"
                                           + "        \"value\" : \"11.0\",\r\n"
                                           + "        \"qualifiers\" : [ \"A\" ],\r\n"
                                           + "        \"dateTime\" : \"2018-09-30T18:00:00.000-06:00\"\r\n"
                                           + "      }, {\r\n"
                                           + "        \"value\" : \"11.0\",\r\n"
                                           + "        \"qualifiers\" : [ \"A\" ],\r\n"
                                           + "        \"dateTime\" : \"2018-09-30T18:15:00.000-06:00\"\r\n"
                                           + "      }, {\r\n"
                                           + "        \"value\" : \"11.0\",\r\n"
                                           + "        \"qualifiers\" : [ \"A\" ],\r\n"
                                           + "        \"dateTime\" : \"2018-09-30T18:30:00.000-06:00\"\r\n"
                                           + "      }, {\r\n"
                                           + "        \"value\" : \"11.0\",\r\n"
                                           + "        \"qualifiers\" : [ \"A\" ],\r\n"
                                           + "        \"dateTime\" : \"2018-09-30T18:45:00.000-06:00\"\r\n"
                                           + "      }, {\r\n"
                                           + "        \"value\" : \"11.5\",\r\n"
                                           + "        \"qualifiers\" : [ \"A\" ],\r\n"
                                           + "        \"dateTime\" : \"2018-09-30T19:00:00.000-06:00\"\r\n"
                                           + "      } ],\r\n"
                                           + "      \"qualifier\" : [ {\r\n"
                                           + "        \"qualifierCode\" : \"A\",\r\n"
                                           + "        \"qualifierDescription\" : \"Approved for publication -- Processing and review completed.\",\r\n"
                                           + "        \"qualifierID\" : 0,\r\n"
                                           + "        \"network\" : \"NWIS\",\r\n"
                                           + "        \"vocabulary\" : \"uv_rmk_cd\"\r\n"
                                           + "      } ],\r\n"
                                           + "      \"qualityControlLevel\" : [ ],\r\n"
                                           + "      \"method\" : [ {\r\n"
                                           + "        \"methodDescription\" : \"\",\r\n"
                                           + "        \"methodID\" : 211948\r\n"
                                           + "      } ],\r\n"
                                           + "      \"source\" : [ ],\r\n"
                                           + "      \"offset\" : [ ],\r\n"
                                           + "      \"sample\" : [ ],\r\n"
                                           + "      \"censorCode\" : [ ]\r\n"
                                           + "    } ],\r\n"
                                           + "    \"name\" : \"USGS:09165000:00060:00000\"\r\n"
                                           + "  } ]\r\n"
                                           + "},\r\n"
                                           + "\"nil\" : false,\r\n"
                                           + "\"globalScope\" : true,\r\n"
                                           + "\"typeSubstituted\" : false\r\n"
                                           + "}";

    private static final String GET = "GET";

    @BeforeEach
    void startServer()
    {
        this.mockServer = ClientAndServer.startClientAndServer( 0 );
    }

    @AfterEach
    void stopServer()
    {
        this.mockServer.stop();
    }

    @Test
    void testReadReturnsOneTimeSeries()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             null,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WATERML,
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
                                               fakeUri,
                                               LeftOrRightOrBaseline.LEFT );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        NwisReader reader = NwisReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            Feature featureKey = Feature.of( MessageFactory.getGeometry( "09165000",
                                                                         "DOLORES RIVER BELOW RICO, CO.",
                                                                         4326,
                                                                         "POINT ( -108.0603517 37.63888428 )" ) );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                                 TimeScaleOuter.of(),
                                                                 "00060",
                                                                 featureKey,
                                                                 "ft3/s" );
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2018-10-01T00:00:00Z" ),
                                                                         11.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2018-10-01T00:15:00Z" ),
                                                                         11.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2018-10-01T00:30:00Z" ),
                                                                         11.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2018-10-01T00:45:00Z" ),
                                                                         11.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2018-10-01T01:00:00Z" ),
                                                                         11.5 ) )
                                                    .setMetadata( metadata )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadReturnsThreeChunkedTimeSeries()
    {
        // Create the chunk parameters
        Parameters parametersOne = new Parameters( new Parameter( "indent", "on" ),
                                                   new Parameter( "endDT", "2019-01-01T00:00:00Z" ),
                                                   new Parameter( "format", "json" ),
                                                   new Parameter( "parameterCd", "00060" ),
                                                   new Parameter( "sites", "09165000" ),
                                                   new Parameter( "startDT", "2018-01-01T00:00:01Z" ) );

        Parameters parametersTwo = new Parameters( new Parameter( "indent", "on" ),
                                                   new Parameter( "endDT", "2020-01-01T00:00:00Z" ),
                                                   new Parameter( "format", "json" ),
                                                   new Parameter( "parameterCd", "00060" ),
                                                   new Parameter( "sites", "09165000" ),
                                                   new Parameter( "startDT", "2019-01-01T00:00:01Z" ) );

        Parameters parametersThree = new Parameters( new Parameter( "indent", "on" ),
                                                     new Parameter( "endDT", "2021-01-01T00:00:00Z" ),
                                                     new Parameter( "format", "json" ),
                                                     new Parameter( "parameterCd", "00060" ),
                                                     new Parameter( "sites", "09165000" ),
                                                     new Parameter( "startDT", "2020-01-01T00:00:01Z" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersOne )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersTwo )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersThree )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             null,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WATERML,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( null,
                                                                     List.of( fakeDeclarationSource ),
                                                                     new Variable( "00060", null ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               fakeUri,
                                               LeftOrRightOrBaseline.LEFT );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                null,
                                                List.of( new NamedFeature( "09165000", null, null ) ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                new DateCondition( "2018-01-01T00:00:00Z", "2021-01-01T00:00:00Z" ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        NwisReader reader = NwisReader.of( pairConfig, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            // Three chunks expected
            assertEquals( 3, actual.size() );
        }

        // Three requests made
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH ),
                                VerificationTimes.exactly( 3 ) );

        // One request made with parameters one
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersOne ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters two
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersTwo ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters three
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersThree ),
                                VerificationTimes.exactly( 1 ) );

    }

}
