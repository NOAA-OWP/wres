package wres.reading.wrds.nwm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Tests {@link WrdsNwmForecastRecord}.
 *
 * @author James Brown
 */

class WrdsNwmForecastRecordTest
{
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder().build();

    /** The exact example payload supplied for this schema: two forecast issuances, each a two-member ensemble. */
    private static final String TWO_ENSEMBLE_FORECASTS = """
            [
              {
                "nwm_feature_id": 5907079,
                "configuration": "medium_range",
                "reference_datetime": "2026-07-08T12:00:00Z",
                "data_type": "streamflow",
                "units": "CMS",
                "forecast": [
                  {
                    "member_id": 1,
                    "timeseries": [
                      { "valid_datetime": "2026-07-08T13:00:00Z", "value": 1.0 },
                      { "valid_datetime": "2026-07-08T14:00:00Z", "value": 2.0 }
                    ]
                  },
                  {
                    "member_id": 2,
                    "timeseries": [
                      { "valid_datetime": "2026-07-08T13:00:00Z", "value": 3.0 },
                      { "valid_datetime": "2026-07-08T14:00:00Z", "value": 4.0 }
                    ]
                  }
                ]
              },
              {
                "nwm_feature_id": 5907079,
                "configuration": "medium_range",
                "reference_datetime": "2026-07-08T18:00:00Z",
                "data_type": "streamflow",
                "units": "CMS",
                "forecast": [
                  {
                    "member_id": 1,
                    "timeseries": [
                      { "valid_datetime": "2026-07-08T19:00:00Z", "value": 5.0 },
                      { "valid_datetime": "2026-07-08T20:00:00Z", "value": 6.0 }
                    ]
                  },
                  {
                    "member_id": 2,
                    "timeseries": [
                      { "valid_datetime": "2026-07-08T19:00:00Z", "value": 7.0 },
                      { "valid_datetime": "2026-07-08T20:00:00Z", "value": 8.0 }
                    ]
                  }
                ]
              }
            ]
            """;

    @Test
    void testDeserializesTwoEnsembleForecastRecordsFromExampleSchema()
    {
        WrdsNwmForecastRecord[] records = OBJECT_MAPPER.readValue( TWO_ENSEMBLE_FORECASTS,
                                                                   WrdsNwmForecastRecord[].class );

        assertEquals( 2, records.length );

        WrdsNwmForecastRecord first = records[0];

        assertEquals( 5907079L, first.nwmFeatureId() );
        assertEquals( "medium_range", first.configuration() );
        assertEquals( Instant.parse( "2026-07-08T12:00:00Z" ), first.referenceDatetime() );
        assertEquals( "streamflow", first.dataType() );
        assertEquals( "CMS", first.units() );
        assertTrue( first.isEnsemble(), "A record with two members should be an ensemble forecast." );
        assertEquals( 2, first.members()
                              .size() );

        WrdsNwmForecastMember firstMemberOfFirstRecord = first.members()
                                                              .get( 0 );
        assertEquals( 1, firstMemberOfFirstRecord.memberId() );
        assertEquals( 2, firstMemberOfFirstRecord.events()
                                                 .size() );
        assertEquals( new WrdsNwmForecastEvent( Instant.parse( "2026-07-08T13:00:00Z" ), 1.0 ),
                      firstMemberOfFirstRecord.events()
                                              .get( 0 ) );
        assertEquals( new WrdsNwmForecastEvent( Instant.parse( "2026-07-08T14:00:00Z" ), 2.0 ),
                      firstMemberOfFirstRecord.events()
                                              .get( 1 ) );

        WrdsNwmForecastMember secondMemberOfFirstRecord = first.members()
                                                               .get( 1 );
        assertEquals( 2, secondMemberOfFirstRecord.memberId() );
        assertEquals( 2, secondMemberOfFirstRecord.events()
                                                  .size() );
        assertEquals( new WrdsNwmForecastEvent( Instant.parse( "2026-07-08T13:00:00Z" ), 3.0 ),
                      secondMemberOfFirstRecord.events()
                                               .get( 0 ) );
        assertEquals( new WrdsNwmForecastEvent( Instant.parse( "2026-07-08T14:00:00Z" ), 4.0 ),
                      secondMemberOfFirstRecord.events()
                                               .get( 1 ) );

        WrdsNwmForecastRecord second = records[1];

        WrdsNwmForecastMember firstMemberOfSecondRecord = second.members()
                                                                .get( 0 );
        assertEquals( 1, firstMemberOfSecondRecord.memberId() );
        assertEquals( 2, firstMemberOfSecondRecord.events()
                                                  .size() );
        assertEquals( new WrdsNwmForecastEvent( Instant.parse( "2026-07-08T19:00:00Z" ), 5.0 ),
                      firstMemberOfSecondRecord.events()
                                               .get( 0 ) );
        assertEquals( new WrdsNwmForecastEvent( Instant.parse( "2026-07-08T20:00:00Z" ), 6.0 ),
                      firstMemberOfSecondRecord.events()
                                               .get( 1 ) );

        WrdsNwmForecastMember secondMemberOfSecondRecord = second.members()
                                                                 .get( 1 );
        assertEquals( 2, secondMemberOfSecondRecord.memberId() );
        assertEquals( 2, secondMemberOfSecondRecord.events()
                                                   .size() );
        assertEquals( new WrdsNwmForecastEvent( Instant.parse( "2026-07-08T19:00:00Z" ), 7.0 ),
                      secondMemberOfSecondRecord.events()
                                                .get( 0 ) );
        assertEquals( new WrdsNwmForecastEvent( Instant.parse( "2026-07-08T20:00:00Z" ), 8.0 ),
                      secondMemberOfSecondRecord.events()
                                                .get( 1 ) );
    }

    @Test
    void testSingleMemberForecastIsNotEnsemble()
    {
        String json = """
                {
                  "nwm_feature_id": 123,
                  "configuration": "medium_range",
                  "reference_datetime": "2026-07-08T12:00:00Z",
                  "data_type": "streamflow",
                  "units": "CMS",
                  "forecast": [
                    {
                      "member_id": 1,
                      "timeseries": [
                        { "valid_datetime": "2026-07-08T19:00:00Z", "value": 5.0 }
                      ]
                    }
                  ]
                }
                """;

        WrdsNwmForecastRecord innerRecord = OBJECT_MAPPER.readValue( json, WrdsNwmForecastRecord.class );

        assertFalse( innerRecord.isEnsemble(), "A record with exactly one member should not be an ensemble "
                                               + "forecast." );
        assertEquals( 1, innerRecord.members()
                                    .size() );
    }

    @Test
    void testMissingForecastArrayResultsInEmptyMembersList()
    {
        String json = """
                {
                  "nwm_feature_id": 123,
                  "configuration": "medium_range",
                  "reference_datetime": "2026-07-08T12:00:00Z",
                  "data_type": "streamflow",
                  "units": "CMS"
                }
                """;

        WrdsNwmForecastRecord innerRecord = OBJECT_MAPPER.readValue( json, WrdsNwmForecastRecord.class );

        assertTrue( innerRecord.members().isEmpty() );
        assertFalse( innerRecord.isEnsemble() );
    }

    @Test
    void testUnknownFieldsAreIgnored()
    {
        String json = """
                {
                  "nwm_feature_id": 123,
                  "configuration": "medium_range",
                  "reference_datetime": "2026-07-08T12:00:00Z",
                  "data_type": "streamflow",
                  "units": "CMS",
                  "some_new_field_the_service_added_later": "should not break parsing",
                  "forecast": [
                    {
                      "member_id": 1,
                      "another_unexpected_field": 42,
                      "timeseries": [
                        { "valid_datetime": "2026-07-08T19:00:00Z", "value": 5.0 }
                      ]
                    }
                  ]
                }
                """;

        WrdsNwmForecastRecord innerRecord = OBJECT_MAPPER.readValue( json, WrdsNwmForecastRecord.class );

        assertEquals( 123L, innerRecord.nwmFeatureId() );
        assertEquals( 1, innerRecord.members().size() );
    }

    @Test
    void testMembersListIsImmutable()
    {
        String json = """
                {
                  "nwm_feature_id": 123,
                  "configuration": "medium_range",
                  "reference_datetime": "2026-07-08T12:00:00Z",
                  "data_type": "streamflow",
                  "units": "CMS",
                  "forecast": [
                    { "member_id": 1, "timeseries": [] }
                  ]
                }
                """;

        WrdsNwmForecastRecord innerRecord = OBJECT_MAPPER.readValue( json, WrdsNwmForecastRecord.class );
        List<WrdsNwmForecastMember> members = innerRecord.members();

        WrdsNwmForecastMember member = new WrdsNwmForecastMember( 2,
                                                                  List.of() );

        Assertions.assertThrows( UnsupportedOperationException.class,
                                 () -> members.add( member ) );
    }
}
