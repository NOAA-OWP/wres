package wres.config.yaml;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisTimes;
import wres.config.yaml.components.AnalysisTimesBuilder;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.SeasonBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.Variable;
import wres.config.yaml.components.VariableBuilder;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link DeclarationUtilitiesTest}.
 * @author James Brown
 */
class DeclarationUtilitiesTest
{
    private static final String INSTANT_ONE = "2017-08-08T00:00:00Z";
    private static final String INSTANT_TWO = "2017-08-08T23:00:00Z";
    private static final String INSTANT_THREE = "2017-08-09T17:00:00Z";
    private static final String INSTANT_FOUR = "2017-08-08T01:00:00Z";
    private static final String INSTANT_FIVE = "2017-08-08T02:00:00Z";
    private static final String INSTANT_SIX = "2017-08-08T03:00:00Z";
    private static final String INSTANT_SEVEN = "2017-08-08T04:00:00Z";
    private static final String INSTANT_EIGHT = "2017-08-08T05:00:00Z";
    private static final String INSTANT_NINE = "2551-03-17T00:00:00Z";
    private static final String INSTANT_TEN = "2551-03-20T00:00:00Z";
    private static final String INSTANT_ELEVEN = "2551-03-19T00:00:00Z";
    private static final String INSTANT_TWELVE = "2551-03-17T13:00:00Z";
    private static final String INSTANT_THIRTEEN = "2551-03-17T07:00:00Z";
    private static final String INSTANT_FOURTEEN = "2551-03-17T20:00:00Z";
    private static final String INSTANT_FIFTEEN = "2551-03-17T14:00:00Z";
    private static final String INSTANT_SIXTEEN = "2551-03-18T03:00:00Z";
    private static final String INSTANT_SEVENTEEN = "2551-03-17T21:00:00Z";
    private static final String INSTANT_EIGHTEEN = "2551-03-18T10:00:00Z";
    private static final String INSTANT_NINETEEN = "2551-03-18T04:00:00Z";
    private static final String INSTANT_TWENTY = "2551-03-18T17:00:00Z";
    private static final String INSTANT_TWENTY_ONE = "2551-03-18T11:00:00Z";
    private static final String INSTANT_TWENTY_TWO = "2551-03-18T18:00:00Z";
    private static final String INSTANT_TWENTY_THREE = "2551-03-19T07:00:00Z";
    private static final String INSTANT_TWENTY_FOUR = "2551-03-19T01:00:00Z";
    private static final String INSTANT_TWENTY_FIVE = "2551-03-19T14:00:00Z";
    private static final String INSTANT_TWENTY_SIX = "2551-03-19T08:00:00Z";
    private static final String INSTANT_TWENTY_SEVEN = "2551-03-19T21:00:00Z";
    private static final String INSTANT_TWENTY_EIGHT = "2551-03-24T00:00:00Z";

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code> and a <code>lead_time_pools</code>. Expects twenty-four time windows
     * with prescribed characteristics.
     *
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated
     * with system test scenario017, as of commit fa548da9da85b16631f238f78b358d85ddbebed5.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsReturnsTwentyFourWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 24 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 1 ) )
                                                  .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 24 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 0 ),
                                                               Duration.ofHours( 1 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 1 ),
                                                               Duration.ofHours( 2 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 2 ),
                                                               Duration.ofHours( 3 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 3 ),
                                                               Duration.ofHours( 4 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 4 ),
                                                               Duration.ofHours( 5 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 5 ),
                                                               Duration.ofHours( 6 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 6 ),
                                                               Duration.ofHours( 7 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 7 ),
                                                               Duration.ofHours( 8 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 8 ),
                                                               Duration.ofHours( 9 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 9 ),
                                                               Duration.ofHours( 10 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 10 ),
                                                               Duration.ofHours( 11 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 11 ),
                                                               Duration.ofHours( 12 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 12 ),
                                                               Duration.ofHours( 13 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 13 ),
                                                               Duration.ofHours( 14 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 14 ),
                                                               Duration.ofHours( 15 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 15 ),
                                                               Duration.ofHours( 16 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 16 ),
                                                               Duration.ofHours( 17 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 17 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 18 ),
                                                               Duration.ofHours( 19 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 19 ),
                                                               Duration.ofHours( 20 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 20 ),
                                                               Duration.ofHours( 21 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 21 ),
                                                               Duration.ofHours( 22 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 22 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 23 ),
                                                               Duration.ofHours( 24 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 24, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code> and a <code>lead_time_pools</code>. Expects two time windows with
     * prescribed characteristics.
     *
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated
     * with system test scenario403, as of commit fa548da9da85b16631f238f78b358d85ddbebed5.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsReturnsTwoWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 48 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 24 ) )
                                                  .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 2 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 0 ),
                                                               Duration.ofHours( 24 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Duration.ofHours( 24 ),
                                                               Duration.ofHours( 48 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 2, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code>, a <code>lead_time_pools</code>, an <code>reference_dates</code>, and an
     * <code>referenceDatesPoolingWindow</code>. Expects eighteen time windows with prescribed characteristics.
     *
     * <p>The project declaration from this test scenario matches (in all important ways) the declaration associated
     * with system test scenario505, which is in development as of commit 766c6d0b4ad96f191bcafb8f2a357c0f2e6a2d3c.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesReferenceDatesLeadTimePoolsAndReferenceDatesPoolingWindowReturnsEighteenWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 40 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 23 ) )
                                                  .frequency( Duration.ofHours( 17 ) )
                                                  .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_NINE ) )
                                                         .maximum( Instant.parse( INSTANT_TEN ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 18 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                               //2551-03-17T00:00:00Z
                                                               Instant.parse( INSTANT_TWELVE ),
                                                               //2551-03-17T13:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                               //2551-03-17T00:00:00Z
                                                               Instant.parse( INSTANT_TWELVE ),
                                                               //2551-03-17T13:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                               //2551-03-17T07:00:00Z
                                                               Instant.parse( INSTANT_FOURTEEN ),
                                                               //2551-03-17T20:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                               //2551-03-17T07:00:00Z
                                                               Instant.parse( INSTANT_FOURTEEN ),
                                                               //2551-03-17T20:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                               //2551-03-17T14:00:00Z
                                                               Instant.parse( INSTANT_SIXTEEN ),
                                                               //2551-03-18T03:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                               //2551-03-17T14:00:00Z
                                                               Instant.parse( INSTANT_SIXTEEN ),
                                                               //2551-03-18T03:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                               //2551-03-17T21:00:00Z
                                                               Instant.parse( INSTANT_EIGHTEEN ),
                                                               //2551-03-18T10:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                               //2551-03-17T21:00:00Z
                                                               Instant.parse( INSTANT_EIGHTEEN ),
                                                               //2551-03-18T10:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                               //2551-03-18T04:00:00Z
                                                               Instant.parse( INSTANT_TWENTY ),
                                                               //2551-03-18T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                               //2551-03-18T04:00:00Z
                                                               Instant.parse( INSTANT_TWENTY ),
                                                               //2551-03-18T17:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                               //2551-03-18T11:00:00Z
                                                               Instant.parse( INSTANT_ELEVEN ),
                                                               //2551-03-19T00:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                               //2551-03-18T11:00:00Z
                                                               Instant.parse( INSTANT_ELEVEN ),
                                                               //2551-03-19T00:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                               //2551-03-18T18:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_THREE ),
                                                               //2551-03-19T07:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                               //2551-03-18T18:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_THREE ),
                                                               //2551-03-19T07:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                               //2551-03-19T01:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_FIVE ),
                                                               //2551-03-19T14:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                               //2551-03-19T01:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_FIVE ),
                                                               //2551-03-19T14:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
                                                               //2551-03-19T08:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_SEVEN ),
                                                               //2551-03-19T21:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
                                                               //2551-03-19T08:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_SEVEN ),
                                                               //2551-03-19T21:00:00Z
                                                               Duration.ofHours( 17 ),
                                                               Duration.ofHours( 40 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 18, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code>, a <code>lead_time_pools</code>, a <code>dates</code> and an
     * <code>reference_dates</code>. Expects one time window with prescribed characteristics.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesValidDatesReferenceDatesAndLeadTimePoolsReturnsOneWindow()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 1 ) )
                                                            .maximum( Duration.ofHours( 48 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 24 ) )
                                                  .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_NINE ) )
                                                         .maximum( Instant.parse( INSTANT_TEN ) )
                                                         .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ELEVEN ) )
                                                     .maximum( Instant.parse( INSTANT_TWENTY_EIGHT ) )
                                                     .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .validDates( validDates )
                                                                        .leadTimePools( leadTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                               //2551-03-17T00:00:00Z
                                                               Instant.parse( INSTANT_TEN ),
                                                               //2551-03-20T00:00:00Z
                                                               Instant.parse( INSTANT_ELEVEN ),
                                                               //2551-03-19T00:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_EIGHT ),
                                                               //2551-03-24T00:00:00Z
                                                               Duration.ofHours( 1 ),
                                                               Duration.ofHours( 25 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * does not include any constraints on time, aka "one big pool".
     *
     * <p>This is analogous to system test scenario508, as of commit b9a7214ec22999482784119a8527149348c80119.
     */

    @Test
    void testGetTimeWindowsFromUnconstrainedDeclarationReturnsOneWindow()
    {
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.MIN,
                                                               Instant.MAX,
                                                               MessageFactory.DURATION_MIN,
                                                               MessageFactory.DURATION_MAX ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>referenceDatesPoolingWindow</code> and a <code>lead_time_pools</code>. Expects twenty-three
     * time windows. Tests both an explicit and implicit declaration of the <code>frequency</code>.
     *
     * <p>The project declaration from this test matches the declaration associated
     * with system test scenario704, as of commit da07c16148429740496b8cc6df89a73e3697f17c,
     * except the <code>period</code> is 1.0 time units here.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesValidDatesReferenceDatesReferenceDatesPoolingWindowAndLeadTimePoolsReturnsTwentyThreeWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 18 ) )
                                                  .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_ONE ) )
                                                         .maximum( Instant.parse( INSTANT_TWO ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 1 ) )
                                                       .frequency( Duration.ofHours( 1 ) )
                                                       .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_THREE ) )
                                                     .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .validDates( validDates )
                                                                        .build();

        // Generate the expected time windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 23 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_FOUR ),
                                                               //2017-08-08T01:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FOUR ),
                                                               //2017-08-08T01:00:00Z
                                                               Instant.parse( INSTANT_FIVE ),
                                                               //2017-08-08T02:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FIVE ),
                                                               //2017-08-08T02:00:00Z
                                                               Instant.parse( INSTANT_SIX ),
                                                               //2017-08-08T03:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SIX ),
                                                               //2017-08-08T03:00:00Z
                                                               Instant.parse( INSTANT_SEVEN ),
                                                               //2017-08-08T04:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SEVEN ),
                                                               //2017-08-08T04:00:00Z
                                                               Instant.parse( INSTANT_EIGHT ),
                                                               //2017-08-08T05:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_EIGHT ),
                                                               //2017-08-08T05:00:00Z
                                                               Instant.parse( "2017-08-08T06:00:00Z" ),
                                                               //2017-08-08T06:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T06:00:00Z" ),
                                                               //2017-08-08T06:00:00Z
                                                               Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               //2017-08-08T07:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               //2017-08-08T07:00:00Z
                                                               Instant.parse( "2017-08-08T08:00:00Z" ),
                                                               //2017-08-08T08:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T08:00:00Z" ),
                                                               //2017-08-08T08:00:00Z
                                                               Instant.parse( "2017-08-08T09:00:00Z" ),
                                                               //2017-08-08T09:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T09:00:00Z" ),
                                                               //2017-08-08T09:00:00Z
                                                               Instant.parse( "2017-08-08T10:00:00Z" ),
                                                               //2017-08-08T10:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T10:00:00Z" ),
                                                               //2017-08-08T10:00:00Z
                                                               Instant.parse( "2017-08-08T11:00:00Z" ),
                                                               //2017-08-08T11:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T11:00:00Z" ),
                                                               //2017-08-08T11:00:00Z
                                                               Instant.parse( "2017-08-08T12:00:00Z" ),
                                                               //2017-08-08T12:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T12:00:00Z" ),
                                                               //2017-08-08T12:00:00Z
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               //2017-08-08T13:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               //2017-08-08T13:00:00Z
                                                               Instant.parse( "2017-08-08T14:00:00Z" ),
                                                               //2017-08-08T14:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T14:00:00Z" ),
                                                               //2017-08-08T14:00:00Z
                                                               Instant.parse( "2017-08-08T15:00:00Z" ),
                                                               //2017-08-08T15:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T15:00:00Z" ),
                                                               //2017-08-08T15:00:00Z
                                                               Instant.parse( "2017-08-08T16:00:00Z" ),
                                                               //2017-08-08T16:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T16:00:00Z" ),
                                                               //2017-08-08T16:00:00Z
                                                               Instant.parse( "2017-08-08T17:00:00Z" ),
                                                               //2017-08-08T17:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T17:00:00Z" ),
                                                               //2017-08-08T17:00:00Z
                                                               Instant.parse( "2017-08-08T18:00:00Z" ),
                                                               //2017-08-08T18:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T18:00:00Z" ),
                                                               //2017-08-08T18:00:00Z
                                                               Instant.parse( "2017-08-08T19:00:00Z" ),
                                                               //2017-08-08T19:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T19:00:00Z" ),
                                                               //2017-08-08T19:00:00Z
                                                               Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               //2017-08-08T20:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               //2017-08-08T20:00:00Z
                                                               Instant.parse( "2017-08-08T21:00:00Z" ),
                                                               //2017-08-08T21:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T21:00:00Z" ),
                                                               //2017-08-08T21:00:00Z
                                                               Instant.parse( "2017-08-08T22:00:00Z" ),
                                                               //2017-08-08T22:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T22:00:00Z" ),
                                                               //2017-08-08T22:00:00Z
                                                               Instant.parse( INSTANT_TWO ),
                                                               //2017-08-08T23:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );

        // Generate the actual time windows for the explicit test
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 23, actualTimeWindows.size() );

        // Assert that the expected and actual time windows are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );

        // Declare the same version of this test with implicit frequency
        TimePools referenceTimePoolsNoFreq = TimePoolsBuilder.builder()
                                                             .period( Duration.ofHours( 1 ) )
                                                             .build();
        EvaluationDeclaration declarationNoFreq = EvaluationDeclarationBuilder.builder()
                                                                              .leadTimes( leadTimes )
                                                                              .leadTimePools( leadTimePools )
                                                                              .referenceDates( referenceDates )
                                                                              .referenceDatePools(
                                                                                      referenceTimePoolsNoFreq )
                                                                              .validDates( validDates )
                                                                              .build();

        // Generate the actual time windows for the implicit test
        Set<TimeWindow> actualTimeWindowsNoFreq = DeclarationUtilities.getTimeWindows( declarationNoFreq );

        // Assert that the expected and actual time windows are equal
        assertEquals( expectedTimeWindows, actualTimeWindowsNoFreq );
    }

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * does not include any explicit time windows, but is constrained by <code>lead_times</code>,
     * <code>reference_dates</code> and <code>dates</code>, aka "one big pool" with constraints.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesValidDatesAndReferenceDatesReturnsOneWindow()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_ONE ) )
                                                         .maximum( Instant.parse( INSTANT_TWO ) )
                                                         .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_THREE ) )
                                                     .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .referenceDates( referenceDates )
                                                                        .validDates( validDates )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_TWO ),
                                                               //2017-08-08T23:00:00Z
                                                               Instant.parse( INSTANT_ONE ),
                                                               //2017-08-08T00:00:00Z
                                                               Instant.parse( INSTANT_THREE ),
                                                               //2017-08-09T17:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code>, a an <code>reference_dates</code>, and an <code>referenceDatesPoolingWindow</code>.
     * Expects nine time windows with prescribed characteristics.
     *
     * <p>The project declaration from this test scenario is similar to the declaration associated
     * with system test scenario505, as of commit c8def0cf2d608c0617786f7cb4f28b563960d667, but without
     * a <code>lead_time_pools</code>.
     */

    @Test
    void testGetTimeWindowsWithLeadTimesReferenceDatesAndReferenceDatesPoolingWindowAndNoLeadTimePoolsReturnsNineWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 40 ) )
                                                            .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_NINE ) )
                                                         .maximum( Instant.parse( INSTANT_TEN ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 9 );

        Duration first = Duration.ofHours( 0 );
        Duration last = Duration.ofHours( 40 );

        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINE ),
                                                               //2551-03-17T00:00:00Z
                                                               Instant.parse( INSTANT_TWELVE ),
                                                               //2551-03-17T13:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_THIRTEEN ),
                                                               //2551-03-17T07:00:00Z
                                                               Instant.parse( INSTANT_FOURTEEN ),
                                                               //2551-03-17T20:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_FIFTEEN ),
                                                               //2551-03-17T14:00:00Z
                                                               Instant.parse( INSTANT_SIXTEEN ),
                                                               //2551-03-18T03:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_SEVENTEEN ),
                                                               //2551-03-17T21:00:00Z
                                                               Instant.parse( INSTANT_EIGHTEEN ),
                                                               //2551-03-18T10:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_NINETEEN ),
                                                               //2551-03-18T04:00:00Z
                                                               Instant.parse( INSTANT_TWENTY ),
                                                               //2551-03-18T17:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_ONE ),
                                                               //2551-03-18T11:00:00Z
                                                               Instant.parse( INSTANT_ELEVEN ),
                                                               //2551-03-19T00:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_TWO ),
                                                               //2551-03-18T18:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_THREE ),
                                                               //2551-03-19T07:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_FOUR ),
                                                               //2551-03-19T01:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_FIVE ),
                                                               //2551-03-19T14:00:00Z
                                                               first,
                                                               last ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( INSTANT_TWENTY_SIX ),
                                                               //2551-03-19T08:00:00Z
                                                               Instant.parse( INSTANT_TWENTY_SEVEN ),
                                                               //2551-03-19T21:00:00Z
                                                               first,
                                                               last ) );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 9, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * <p>Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} where the project declaration
     * includes a <code>lead_times</code> and an <code>lead_time_pools</code> and the <code>minimum</code> and
     * <code>maximum</code> lead hours are the same value and the period associated with the
     * <code>lead_time_pools</code> is zero wide. This is equivalent to system test scenario010 as of commit
     * 8480aa4d4ddc09275746fe590623ecfd83e452ae and is used to check that a zero-wide pool centered on a single lead
     * duration does not increment infinitely.
     */

    @Test
    void testGetTimeWindowsWithZeroWideLeadTimesAndLeadTimePoolsWithZeroPeriodReturnsOneWindow()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 43 ) )
                                                            .maximum( Duration.ofHours( 43 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 0 ) )
                                                  .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 1 );

        Duration first = Duration.ofHours( 43 );
        Duration last = Duration.ofHours( 43 );

        TimeWindow inner = MessageFactory.getTimeWindow( first,
                                                         last );
        expectedTimeWindows.add( inner );

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 1, actualTimeWindows.size() );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    @Test
    void testGetTimeWindowsWithValidDatesAndValidDatePoolsReturnsTwoWindows()
    {
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 2, actualTimeWindows.size() );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 2 );

        TimeWindow innerOne = MessageFactory.getTimeWindow( Instant.parse( INSTANT_ONE ),
                                                            Instant.parse( "2017-08-08T13:00:00Z" ) );
        TimeWindow innerTwo = MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T07:00:00Z" ),
                                                            Instant.parse( "2017-08-08T20:00:00Z" ) );

        expectedTimeWindows.add( innerOne );
        expectedTimeWindows.add( innerTwo );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsAndValidDatesAndValidDatePoolsReturnsFourWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 19 ) )
                                                            .maximum( Duration.ofHours( 34 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 8 ) )
                                                  .frequency( Duration.ofHours( 7 ) )
                                                  .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 4, actualTimeWindows.size() );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 4 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.parse( INSTANT_ONE ),
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               Duration.ofHours( 19 ),
                                                               Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               Duration.ofHours( 19 ),
                                                               Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.parse( INSTANT_ONE ),
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               Duration.ofHours( 26 ),
                                                               Duration.ofHours( 34 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.MIN,
                                                               Instant.MAX,
                                                               Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               Duration.ofHours( 26 ),
                                                               Duration.ofHours( 34 ) ) );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    @Test
    void testGetTimeWindowsWithLeadTimesAndLeadTimePoolsAndValidDatesAndValidDatePoolsAndReferenceDatesAndReferenceDatePoolsReturnsFourWindows()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 19 ) )
                                                            .maximum( Duration.ofHours( 34 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 8 ) )
                                                  .frequency( Duration.ofHours( 7 ) )
                                                  .build();
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_TWO ) )
                                                         .maximum( Instant.parse( INSTANT_THREE ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 17 ) )
                                                       .frequency( Duration.ofHours( 23 ) )
                                                       .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .maximum( Instant.parse( INSTANT_TWO ) )
                                                     .build();
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        // Generate the actual windows
        Set<TimeWindow> actualTimeWindows = DeclarationUtilities.getTimeWindows( declaration );

        // Assert the expected cardinality
        assertEquals( 4, actualTimeWindows.size() );

        // Generate the expected windows
        Set<TimeWindow> expectedTimeWindows = new HashSet<>( 4 );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                               Instant.parse( "2017-08-09T16:00:00Z" ),
                                                               Instant.parse( INSTANT_ONE ),
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               Duration.ofHours( 19 ),
                                                               Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                               Instant.parse( "2017-08-09T16:00:00Z" ),
                                                               Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               Duration.ofHours( 19 ),
                                                               Duration.ofHours( 27 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                               Instant.parse( "2017-08-09T16:00:00Z" ),
                                                               Instant.parse( INSTANT_ONE ),
                                                               Instant.parse( "2017-08-08T13:00:00Z" ),
                                                               Duration.ofHours( 26 ),
                                                               Duration.ofHours( 34 ) ) );
        expectedTimeWindows.add( MessageFactory.getTimeWindow( Instant.parse( "2017-08-08T23:00:00Z" ),
                                                               Instant.parse( "2017-08-09T16:00:00Z" ),
                                                               Instant.parse( "2017-08-08T07:00:00Z" ),
                                                               Instant.parse( "2017-08-08T20:00:00Z" ),
                                                               Duration.ofHours( 26 ),
                                                               Duration.ofHours( 34 ) ) );

        // Assert that the expected and actual are equal
        assertEquals( expectedTimeWindows, actualTimeWindows );
    }

    /**
     * Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} for an expected exception when
     * <code>lead_times</code> are required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenLeadTimesExpectedButMissing()
    {
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 18 ) )
                                                  .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine lead duration time windows without 'lead_times'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} for an expected exception when the
     * <code>minimum</code> <code>lead_times</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenLeadTimesMinimumExpectedButMissing()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .maximum( Duration.ofHours( 40 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 18 ) )
                                                  .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine lead duration time windows without a 'minimum' value for 'lead_times'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} for an expected exception when the
     * <code>maximum</code> <code>lead_times</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenLeadTimesMaximumExpectedButMissing()
    {
        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( Duration.ofHours( 18 ) )
                                                  .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .leadTimePools( leadTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine lead duration time windows without a 'maximum' value for 'lead_times'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} for an expected exception when
     * <code>reference_dates</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenReferenceDatesExpectedButMissing()
    {
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine reference time windows without 'reference_dates'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} for an expected exception when the
     * <code>minimum</code> <code>reference_dates</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenReferenceDatesEarliestExpectedButMissing()
    {
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .maximum( Instant.parse( INSTANT_ONE ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine reference time windows without the 'minimum' for the 'reference_dates'.",
                      thrown.getMessage() );
    }

    /**
     * Tests the {@link DeclarationUtilities#getTimeWindows(EvaluationDeclaration)} for an expected exception when the
     * <code>maximum</code> <code>reference_dates</code> is required but missing.
     */

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenReferenceDatesLatestExpectedButMissing()
    {
        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( INSTANT_ONE ) )
                                                         .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .referenceDates( referenceDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine reference time windows without the 'maximum' for the 'reference_dates'.",
                      thrown.getMessage() );
    }

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenValidDatesExpectedButMissing()
    {
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine valid time windows without 'valid_dates'.",
                      thrown.getMessage() );
    }

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenValidDatesEarliestExpectedButMissing()
    {
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .maximum( Instant.parse( INSTANT_ONE ) )
                                                     .build();
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine valid time windows without the 'minimum' for the 'valid_dates'.",
                      thrown.getMessage() );
    }

    @Test
    void testGetTimeWindowsThrowsNullPointerExceptionWhenValidDatesLatestExpectedButMissing()
    {
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( INSTANT_ONE ) )
                                                     .build();
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( Duration.ofHours( 13 ) )
                                                   .frequency( Duration.ofHours( 7 ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .validDatePools( validTimePools )
                                                                        .build();

        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> DeclarationUtilities.getTimeWindows( declaration ) );

        assertEquals( "Cannot determine valid time windows without the 'maximum' for the 'valid_dates'.",
                      thrown.getMessage() );
    }

    @Test
    void testHasBaselineReturnsTrue()
    {
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( DatasetBuilder.builder()
                                                                                 .build() )
                                                         .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .baseline( baseline )
                                                                       .build();

        assertTrue( DeclarationUtilities.hasBaseline( evaluation ) );
    }

    @Test
    void testHasBaselineReturnsFalse()
    {
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .build();

        assertFalse( DeclarationUtilities.hasBaseline( evaluation ) );
    }

    @Test
    void testGetFeatures()
    {
        GeometryTuple singleton = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "foo" ) )
                                               .setRight( Geometry.newBuilder()
                                                                  .setName( "bar" ) )
                                               .build();
        GeometryTuple grouped = GeometryTuple.newBuilder()
                                             .setLeft( Geometry.newBuilder()
                                                               .setName( "baz" ) )
                                             .setRight( Geometry.newBuilder()
                                                                .setName( "qux" ) )
                                             .build();

        Features features = new Features( Set.of( singleton ) );
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( Set.of( grouped ) )
                                           .build();
        FeatureGroups featureGroups = new FeatureGroups( Set.of( group ) );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .features( features )
                                                                       .featureGroups( featureGroups )
                                                                       .build();
        Set<GeometryTuple> expected = Set.of( singleton, grouped );

        assertEquals( expected, DeclarationUtilities.getFeatures( evaluation ) );
    }

    @Test
    void testFromEnumName()
    {
        String from = "An_eNum_nAmE";
        String expected = "an enum name";
        assertEquals( expected, DeclarationUtilities.fromEnumName( from ) );
    }

    @Test
    void testToEnumName()
    {
        String from = "an enum name";
        String expected = "AN_ENUM_NAME";
        assertEquals( expected, DeclarationUtilities.toEnumName( from ) );
    }

    @Test
    void testGetDurationInPreferredUnitsReturnsHours()
    {
        Duration duration = Duration.ofHours( 3 );
        Pair<Long, String> preferred = DeclarationUtilities.getDurationInPreferredUnits( duration );
        assertEquals( Pair.of( 3L, "hours" ), preferred );
    }

    @Test
    void testGetDurationInPreferredUnitsReturnsSeconds()
    {
        Duration duration = Duration.ofMinutes( 12 );
        Pair<Long, String> preferred = DeclarationUtilities.getDurationInPreferredUnits( duration );
        assertEquals( Pair.of( 720L, "seconds" ), preferred );
    }

    @Test
    void testGetFeatureNamesFor()
    {
        Geometry left = Geometry.newBuilder()
                                .setName( "foo" )
                                .build();
        Geometry right = Geometry.newBuilder()
                                 .setName( "bar" )
                                 .build();
        Geometry baseline = Geometry.newBuilder()
                                    .setName( "baz" )
                                    .build();

        GeometryTuple singleton = GeometryTuple.newBuilder()
                                               .setLeft( left )
                                               .setRight( right )
                                               .setBaseline( baseline )
                                               .build();

        Set<GeometryTuple> singletonSet = Set.of( singleton );

        assertAll( () -> assertEquals( Set.of( "foo" ),
                                       DeclarationUtilities.getFeatureNamesFor( singletonSet,
                                                                                DatasetOrientation.LEFT ) ),
                   () -> assertEquals( Set.of( "bar" ),
                                       DeclarationUtilities.getFeatureNamesFor( singletonSet,
                                                                                DatasetOrientation.RIGHT ) ),
                   () -> assertEquals( Set.of( "baz" ),
                                       DeclarationUtilities.getFeatureNamesFor( singletonSet,
                                                                                DatasetOrientation.BASELINE ) ) );

    }

    @Test
    void testGetFeatureAuthorityFor()
    {
        Dataset left = DatasetBuilder.builder()
                                     .featureAuthority( FeatureAuthority.USGS_SITE_CODE )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .featureAuthority( FeatureAuthority.NWM_FEATURE_ID )
                                      .build();
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .featureAuthority( FeatureAuthority.NWS_LID )
                                                              .build() )
                                      .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( left )
                                                                       .right( right )
                                                                       .baseline( baseline )
                                                                       .build();

        assertAll( () -> assertEquals( FeatureAuthority.USGS_SITE_CODE,
                                       DeclarationUtilities.getFeatureAuthorityFor( evaluation,
                                                                                    DatasetOrientation.LEFT ) ),
                   () -> assertEquals( FeatureAuthority.NWM_FEATURE_ID,
                                       DeclarationUtilities.getFeatureAuthorityFor( evaluation,
                                                                                    DatasetOrientation.RIGHT ) ),
                   () -> assertEquals( FeatureAuthority.NWS_LID,
                                       DeclarationUtilities.getFeatureAuthorityFor( evaluation,
                                                                                    DatasetOrientation.BASELINE ) ) );
    }

    @Test
    void testHasBaselineBuilderReturnsTrue()
    {
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( DatasetBuilder.builder()
                                                                                 .build() )
                                                         .build();
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder()
                                                                           .baseline( baseline );

        assertTrue( DeclarationUtilities.hasBaseline( builder ) );
    }

    @Test
    void testHasBaselineBuilderReturnsFalse()
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder();

        assertFalse( DeclarationUtilities.hasBaseline( builder ) );
    }

    @Test
    void testGetFeatureAuthoritiesReturnsExplicitAuthorityForEachDataset()
    {
        Dataset left = DatasetBuilder.builder()
                                     .featureAuthority( FeatureAuthority.USGS_SITE_CODE )
                                     .label( "foo" )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .featureAuthority( FeatureAuthority.NWM_FEATURE_ID )
                                      .label( "bar" )
                                      .build();
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .featureAuthority( FeatureAuthority.NWS_LID )
                                                              .label( "baz" )
                                                              .build() )
                                      .build();

        assertAll( () -> assertEquals( Set.of( FeatureAuthority.USGS_SITE_CODE ),
                                       DeclarationUtilities.getFeatureAuthorities( left ) ),
                   () -> assertEquals( Set.of( FeatureAuthority.NWM_FEATURE_ID ),
                                       DeclarationUtilities.getFeatureAuthorities( right ) ),
                   () -> assertEquals( Set.of( FeatureAuthority.NWS_LID ),
                                       DeclarationUtilities.getFeatureAuthorities( baseline.dataset() ) ) );
    }

    @Test
    void testGetFeatureAuthoritiesReturnsInterpolatedAuthorityForEachDataset()
    {
        List<wres.config.yaml.components.Source> leftSources =
                List.of( SourceBuilder.builder()
                                      .sourceInterface( SourceInterface.USGS_NWIS )
                                      .build() );
        Dataset left = DatasetBuilder.builder()
                                     .sources( leftSources )
                                     .label( "foo" )
                                     .build();
        List<wres.config.yaml.components.Source> rightSources =
                List.of( SourceBuilder.builder()
                                      .sourceInterface( SourceInterface.NWM_LONG_RANGE_CHANNEL_RT_CONUS )
                                      .build() );
        Dataset right =
                DatasetBuilder.builder()
                              .sources( rightSources )
                              .label( "bar" )
                              .build();
        List<wres.config.yaml.components.Source> baselineSources =
                List.of( SourceBuilder.builder()
                                      .sourceInterface( SourceInterface.WRDS_AHPS )
                                      .build() );
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .sources( baselineSources )
                                                              .label( "baz" )
                                                              .build() )
                                      .build();

        assertAll( () -> assertEquals( Set.of( FeatureAuthority.USGS_SITE_CODE ),
                                       DeclarationUtilities.getFeatureAuthorities( left ) ),
                   () -> assertEquals( Set.of( FeatureAuthority.NWM_FEATURE_ID ),
                                       DeclarationUtilities.getFeatureAuthorities( right ) ),
                   () -> assertEquals( Set.of( FeatureAuthority.NWS_LID ),
                                       DeclarationUtilities.getFeatureAuthorities( baseline.dataset() ) ) );
    }

    @Test
    void testHasAnalysisDurationsReturnsTrue()
    {
        AnalysisTimes analysisTimes =
                AnalysisTimesBuilder.builder()
                                    .minimum( java.time.Duration.ofHours( 3 ) )
                                    .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .analysisTimes( analysisTimes )
                                                                       .build();

        assertTrue( DeclarationUtilities.hasAnalysisTimes( evaluation ) );
    }

    @Test
    void testHasAnalysisDurationsReturnsFalse()
    {
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .build();

        assertFalse( DeclarationUtilities.hasAnalysisTimes( evaluation ) );
    }

    @Test
    void testGroupThresholdsByType()
    {
        Threshold probability = Threshold.newBuilder()
                                         .setName( "foo" )
                                         .build();
        Threshold value = Threshold.newBuilder()
                                   .setName( "bar" )
                                   .build();
        Threshold classifier = Threshold.newBuilder()
                                        .setName( "foo" )
                                        .build();
        wres.config.yaml.components.Threshold probabilityWrapped =
                wres.config.yaml.components.ThresholdBuilder.builder()
                                                            .type( ThresholdType.PROBABILITY )
                                                            .threshold( probability )
                                                            .build();
        wres.config.yaml.components.Threshold valueWrapped =
                wres.config.yaml.components.ThresholdBuilder.builder()
                                                            .type( ThresholdType.VALUE )
                                                            .threshold( value )
                                                            .build();
        wres.config.yaml.components.Threshold classifierWrapped =
                wres.config.yaml.components.ThresholdBuilder.builder()
                                                            .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                            .threshold( classifier )
                                                            .build();
        Set<wres.config.yaml.components.Threshold> thresholds = Set.of( probabilityWrapped,
                                                                        valueWrapped,
                                                                        classifierWrapped );

        Map<ThresholdType, Set<wres.config.yaml.components.Threshold>> actual =
                DeclarationUtilities.groupThresholdsByType( thresholds );

        Map<ThresholdType, Set<wres.config.yaml.components.Threshold>> expected =
                Map.of( ThresholdType.PROBABILITY, Set.of( probabilityWrapped ),
                        ThresholdType.VALUE, Set.of( valueWrapped ),
                        ThresholdType.PROBABILITY_CLASSIFIER, Set.of( classifierWrapped ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetMetricGroupsForProcessing()
    {
        Set<wres.config.yaml.components.Threshold> thresholdsOne
                = Set.of( ThresholdBuilder.builder()
                                          .threshold( Threshold.newBuilder()
                                                               .setLeftThresholdValue( 23.0 )
                                                               .setOperator( Threshold.ThresholdOperator.GREATER )
                                                               .build() )
                                          .type( ThresholdType.VALUE )
                                          .featureNameFrom( DatasetOrientation.LEFT )
                                          .build() );
        Metric one = MetricBuilder.Metric( MetricConstants.MEAN_ABSOLUTE_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .thresholds( thresholdsOne )
                                                                  .build() );
        Metric two = MetricBuilder.Metric( MetricConstants.MEAN_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .thresholds( thresholdsOne )
                                                                  .build() );

        Set<wres.config.yaml.components.Threshold> thresholdsTwo
                = Set.of( ThresholdBuilder.builder()
                                          .threshold( Threshold.newBuilder()
                                                               .setLeftThresholdValue( 0.3 )
                                                               .setOperator( Threshold.ThresholdOperator.LESS )
                                                               .build() )
                                          .type( ThresholdType.PROBABILITY )
                                          .featureNameFrom( DatasetOrientation.RIGHT )
                                          .build() );

        Metric three = MetricBuilder.Metric( MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                             MetricParametersBuilder.builder()
                                                                    .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                    .probabilityThresholds( thresholdsTwo )
                                                                    .build() );
        Metric four = MetricBuilder.Metric( MetricConstants.MEAN_SQUARE_ERROR,
                                            MetricParametersBuilder.builder()
                                                                   .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                   .probabilityThresholds( thresholdsTwo )
                                                                   .build() );

        Set<Metric> metrics = Set.of( one, two, three, four );

        Set<Set<Metric>> actual = DeclarationUtilities.getMetricGroupsForProcessing( metrics );

        Set<Set<Metric>> expected = Set.of( Set.of( three, four ), Set.of( one, two ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetMetricGroupsForProcessingWithTimingErrorSummaryStatistics()
    {
        Set<SummaryStatistic> summaryStatisticsOne = new LinkedHashSet<>();
        SummaryStatistic.Builder template = SummaryStatistic.newBuilder()
                                                            .setDimension( SummaryStatistic.StatisticDimension.TIMING_ERRORS );
        SummaryStatistic mean = template.setStatistic( SummaryStatistic.StatisticName.MEAN )
                                        .build();
        SummaryStatistic median = template.setStatistic( SummaryStatistic.StatisticName.MEDIAN )
                                          .build();
        SummaryStatistic meanAbsolute = template.setStatistic( SummaryStatistic.StatisticName.MEAN_ABSOLUTE )
                                                .build();
        summaryStatisticsOne.add( mean );
        summaryStatisticsOne.add( median );
        summaryStatisticsOne.add( meanAbsolute );

        MetricParameters parametersOne = MetricParametersBuilder.builder()
                                                                .summaryStatistics( summaryStatisticsOne )
                                                                .build();
        Metric metricOne = MetricBuilder.builder()
                                        .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                        .parameters( parametersOne )
                                        .build();


        Set<SummaryStatistic> summaryStatisticsTwo = new LinkedHashSet<>();
        SummaryStatistic sd = template.setStatistic( SummaryStatistic.StatisticName.STANDARD_DEVIATION )
                                      .build();
        SummaryStatistic minimum = template.setStatistic( SummaryStatistic.StatisticName.MINIMUM )
                                           .build();
        SummaryStatistic maximum = template.setStatistic( SummaryStatistic.StatisticName.MAXIMUM )
                                           .build();
        summaryStatisticsTwo.add( sd );
        summaryStatisticsTwo.add( minimum );
        summaryStatisticsTwo.add( maximum );

        MetricParameters parametersTwo = MetricParametersBuilder.builder()
                                                                .summaryStatistics( summaryStatisticsTwo )
                                                                .build();
        Metric metricTwo = MetricBuilder.builder()
                                        .name( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR )
                                        .parameters( parametersTwo )
                                        .build();
        Set<Set<Metric>> actual = DeclarationUtilities.getMetricGroupsForProcessing( Set.of( metricOne, metricTwo ) );

        Set<Set<Metric>> expected =
                Set.of( Set.of( metricOne,
                                metricTwo,
                                new Metric( MetricConstants.TIME_TO_PEAK_ERROR_MEAN, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_ERROR_MEDIAN, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_ERROR_MEAN_ABSOLUTE, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_MINIMUM, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_MAXIMUM, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STANDARD_DEVIATION, null ) ) );

        assertEquals( expected, actual );
    }

    @Test
    void testAddDataSources()
    {
        // Create some sources with parameters to correlate
        Source leftOne = SourceBuilder.builder()
                                      .uri( URI.create( "foo.csv" ) )
                                      .timeZoneOffset( ZoneOffset.ofHours( 3 ) )
                                      .build();
        Source leftTwo = SourceBuilder.builder()
                                      .uri( URI.create( "qux.csv" ) )
                                      .timeZoneOffset( ZoneOffset.ofHours( 4 ) )
                                      .build();
        List<wres.config.yaml.components.Source> leftSources = List.of( leftOne, leftTwo );
        Dataset left = DatasetBuilder.builder()
                                     .sources( leftSources )
                                     .build();
        Source rightOne = SourceBuilder.builder()
                                       .uri( URI.create( "bar.csv" ) )
                                       .sourceInterface( SourceInterface.NWM_LONG_RANGE_CHANNEL_RT_CONUS )
                                       .build();
        List<wres.config.yaml.components.Source> rightSources = List.of( rightOne );
        Dataset right = DatasetBuilder.builder()
                                      .sources( rightSources )
                                      .build();
        Source baselineOne = SourceBuilder.builder()
                                          .uri( URI.create( "baz.csv" ) )
                                          .missingValue( List.of( -999.0 ) )
                                          .build();
        List<wres.config.yaml.components.Source> baselineSources = List.of( baselineOne );
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .sources( baselineSources )
                                                              .build() )
                                      .build();

        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                  .left( left )
                                                                                  .right( right )
                                                                                  .baseline( baseline )
                                                                                  .build();

        // Create some correlated and some uncorrelated URIs
        URI leftCorrelated = URI.create( "foopath/foo.csv" );
        URI leftUncorrelated = URI.create( "foopath/fooest.csv" );

        URI rightCorrelated = URI.create( "barpath/bar.csv" );
        URI rightUncorrelated = URI.create( "barpath/barest.csv" );

        URI baselineCorrelated = URI.create( "bazpath/baz.csv" );
        URI baselineUncorrelated = URI.create( "bazpath/bazest.csv" );

        List<URI> newLeftSources = List.of( leftCorrelated, leftUncorrelated );
        List<URI> newRightSources = List.of( rightCorrelated, rightUncorrelated );
        List<URI> newBaselineSources = List.of( baselineCorrelated, baselineUncorrelated );

        EvaluationDeclaration actual = DeclarationUtilities.addDataSources( evaluationDeclaration,
                                                                            newLeftSources,
                                                                            newRightSources,
                                                                            newBaselineSources );

        // Create the expectation
        Source leftSourceExpectedOne = SourceBuilder.builder( leftOne )
                                                    .uri( leftCorrelated )
                                                    .build();
        Source leftSourceExpectedTwo = SourceBuilder.builder()
                                                    .uri( leftUncorrelated )
                                                    .build();
        Source rightSourceExpectedOne = SourceBuilder.builder( rightOne )
                                                     .uri( rightCorrelated )
                                                     .build();
        Source rightSourceExpectedTwo = SourceBuilder.builder()
                                                     .uri( rightUncorrelated )
                                                     .build();
        Source baselineSourceExpectedOne = SourceBuilder.builder( baselineOne )
                                                        .uri( baselineCorrelated )
                                                        .build();
        Source baselineSourceExpectedTwo = SourceBuilder.builder()
                                                        .uri( baselineUncorrelated )
                                                        .build();
        List<Source> leftSourcesExpected = List.of( leftTwo, leftSourceExpectedOne, leftSourceExpectedTwo );
        Dataset leftExpected = DatasetBuilder.builder()
                                             .sources( leftSourcesExpected )
                                             .build();
        List<Source> rightSourcesExpected = List.of( rightSourceExpectedOne, rightSourceExpectedTwo );
        Dataset rightExpected = DatasetBuilder.builder()
                                              .sources( rightSourcesExpected )
                                              .build();
        List<Source> baselineSourcesExpected = List.of( baselineSourceExpectedOne, baselineSourceExpectedTwo );
        Dataset baselineDatasetExpected = DatasetBuilder.builder()
                                                        .sources( baselineSourcesExpected )
                                                        .build();
        BaselineDataset baselineExpected =
                BaselineDatasetBuilder.builder()
                                      .dataset( baselineDatasetExpected )
                                      .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( leftExpected )
                                                                     .right( rightExpected )
                                                                     .baseline( baselineExpected )
                                                                     .build();
        assertEquals( expected, actual );
    }

    /**
     * See Redmine issue #116899
     */

    @Test
    void testAddDataSourcesRetainsExistingSources()
    {
        Source baselineOne =
                SourceBuilder.builder()
                             .uri( URI.create( "singleValuedEx_ABRFC_ARCFUL_OBS/FLTA4X.QINE.19951101.20170905.datacard" ) )
                             .build();
        Source baselineTwo =
                SourceBuilder.builder()
                             .uri( URI.create( "singleValuedEx_ABRFC_ARCFUL_OBS/FRSO2X.QINE.19951101.20170905.datacard" ) )
                             .build();

        List<wres.config.yaml.components.Source> baselineSources = List.of( baselineOne, baselineTwo );
        TimeScale timeScaleInner = TimeScale.newBuilder()
                                            .setPeriod( com.google.protobuf.Duration.newBuilder().setSeconds( 1 ) )
                                            .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                            .build();
        wres.config.yaml.components.TimeScale timeScale = new wres.config.yaml.components.TimeScale( timeScaleInner );
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .build();
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .sources( baselineSources )
                                                              .variable( new Variable( "QINE", null ) )
                                                              .type( DataType.OBSERVATIONS )
                                                              .timeZoneOffset( ZoneOffset.ofHours( -6 ) )
                                                              .timeScale( timeScale )
                                                              .build() )
                                      .generatedBaseline( persistence )
                                      .build();

        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                  .baseline( baseline )
                                                                                  .build();

        // Create some correlated and some uncorrelated URIs
        URI sourceOne = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_9291293271822018547" );
        URI sourceTwo = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_11687359385535593111" );
        URI sourceThree = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_16018604822676150580" );
        URI sourceFour = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_14964912810788706087" );
        URI sourceFive = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_4655376427529148367" );
        URI sourceSix = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_9748034963021086804" );
        URI sourceSeven = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_17342198904464396701" );

        List<URI> newBaselineSources = List.of( sourceOne,
                                                sourceTwo,
                                                sourceThree,
                                                sourceFour,
                                                sourceFive,
                                                sourceSix,
                                                sourceSeven );

        EvaluationDeclaration actualEvaluation = DeclarationUtilities.addDataSources( evaluationDeclaration,
                                                                                      List.of(),
                                                                                      List.of(),
                                                                                      newBaselineSources );

        // Create the expectation
        Source baselineSourceExpectedOne = SourceBuilder.builder()
                                                        .uri( sourceOne )
                                                        .build();
        Source baselineSourceExpectedTwo = SourceBuilder.builder()
                                                        .uri( sourceTwo )
                                                        .build();
        Source baselineSourceExpectedThree = SourceBuilder.builder()
                                                          .uri( sourceThree )
                                                          .build();
        Source baselineSourceExpectedFour = SourceBuilder.builder()
                                                         .uri( sourceFour )
                                                         .build();
        Source baselineSourceExpectedFive = SourceBuilder.builder()
                                                         .uri( sourceFive )
                                                         .build();
        Source baselineSourceExpectedSix = SourceBuilder.builder()
                                                        .uri( sourceSix )
                                                        .build();
        Source baselineSourceExpectedSeven = SourceBuilder.builder()
                                                          .uri( sourceSeven )
                                                          .build();

        List<Source> baselineSourcesExpected = List.of( baselineOne,
                                                        baselineTwo,
                                                        baselineSourceExpectedOne,
                                                        baselineSourceExpectedTwo,
                                                        baselineSourceExpectedThree,
                                                        baselineSourceExpectedFour,
                                                        baselineSourceExpectedFive,
                                                        baselineSourceExpectedSix,
                                                        baselineSourceExpectedSeven );
        Dataset baselineDatasetExpected = DatasetBuilder.builder()
                                                        .sources( baselineSourcesExpected )
                                                        .variable( new Variable( "QINE", null ) )
                                                        .type( DataType.OBSERVATIONS )
                                                        .timeZoneOffset( ZoneOffset.ofHours( -6 ) )
                                                        .timeScale( timeScale )
                                                        .build();
        BaselineDataset expected =
                BaselineDatasetBuilder.builder()
                                      .dataset( baselineDatasetExpected )
                                      .generatedBaseline( persistence )
                                      .build();

        BaselineDataset actual = actualEvaluation.baseline();

        assertEquals( expected, actual );
    }

    @Test
    void testAddDataSourcesWithMissingDatasets()
    {
        // Create some sources
        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                  .build();

        // Create some correlated and some uncorrelated URIs
        URI leftSource = URI.create( "foopath/foo.csv" );
        URI rightSource = URI.create( "barpath/bar.csv" );
        URI baselineSource = URI.create( "bazpath/baz.csv" );

        List<URI> newLeftSources = List.of( leftSource );
        List<URI> newRightSources = List.of( rightSource );
        List<URI> newBaselineSources = List.of( baselineSource );

        EvaluationDeclaration actual = DeclarationUtilities.addDataSources( evaluationDeclaration,
                                                                            newLeftSources,
                                                                            newRightSources,
                                                                            newBaselineSources );

        // Create the expectation
        Source leftSourceExpectedOne = SourceBuilder.builder()
                                                    .uri( leftSource )
                                                    .build();
        Source rightSourceExpectedOne = SourceBuilder.builder()
                                                     .uri( rightSource )
                                                     .build();
        Source baselineSourceExpectedOne = SourceBuilder.builder()
                                                        .uri( baselineSource )
                                                        .build();
        List<Source> leftSourcesExpected = List.of( leftSourceExpectedOne );
        Dataset leftExpected = DatasetBuilder.builder()
                                             .sources( leftSourcesExpected )
                                             .build();
        List<Source> rightSourcesExpected = List.of( rightSourceExpectedOne );
        Dataset rightExpected = DatasetBuilder.builder()
                                              .sources( rightSourcesExpected )
                                              .build();
        List<Source> baselineSourcesExpected = List.of( baselineSourceExpectedOne );
        Dataset baselineDatasetExpected = DatasetBuilder.builder()
                                                        .sources( baselineSourcesExpected )
                                                        .build();
        BaselineDataset baselineExpected =
                BaselineDatasetBuilder.builder()
                                      .dataset( baselineDatasetExpected )
                                      .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( leftExpected )
                                                                     .right( rightExpected )
                                                                     .baseline( baselineExpected )
                                                                     .build();
        assertEquals( expected, actual );
    }

    @Test
    void testIsReadableFileReturnsFalseForInvalidPath()
    {
        String path = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                  """;

        FileSystem fileSystem = FileSystems.getDefault();
        assertFalse( DeclarationUtilities.isReadableFile( fileSystem, path ) );
    }

    @Test
    void testIsReadableFileReturnsTrueForReadableFile() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path path = fileSystem.getPath( "foo.file" );
            Files.createFile( path );
            String pathString = path.toString();

            assertTrue( DeclarationUtilities.isReadableFile( fileSystem, pathString ) );
        }
    }

    @Test
    void testGetSourceTimeScales()
    {
        TimeScale scaleOne = TimeScale.newBuilder()
                                      .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                      .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                              .setSeconds( 100 )
                                                                              .build() )
                                      .build();
        wres.config.yaml.components.TimeScale scaleOneWrapped = new wres.config.yaml.components.TimeScale( scaleOne );
        Source leftSourceOne = SourceBuilder.builder()
                                            .build();
        TimeScale scaleTwo = TimeScale.newBuilder()
                                      .setFunction( TimeScale.TimeScaleFunction.TOTAL )
                                      .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                              .setSeconds( 789 )
                                                                              .build() )
                                      .build();
        wres.config.yaml.components.TimeScale scaleTwoWrapped = new wres.config.yaml.components.TimeScale( scaleTwo );
        Source rightSourceOne = SourceBuilder.builder()
                                             .build();
        TimeScale scaleThree = TimeScale.newBuilder()
                                        .setFunction( TimeScale.TimeScaleFunction.MAXIMUM )
                                        .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                                .setSeconds( 1800 )
                                                                                .build() )
                                        .build();
        wres.config.yaml.components.TimeScale scaleThreeWrapped =
                new wres.config.yaml.components.TimeScale( scaleThree );
        Source baselineSourceOne = SourceBuilder.builder()
                                                .build();
        List<Source> leftSources = List.of( leftSourceOne );
        Dataset left = DatasetBuilder.builder()
                                     .sources( leftSources )
                                     .timeScale( scaleOneWrapped )
                                     .build();
        List<Source> rightSources = List.of( rightSourceOne );
        Dataset right = DatasetBuilder.builder()
                                      .sources( rightSources )
                                      .timeScale( scaleTwoWrapped )
                                      .build();
        List<Source> baselineSources = List.of( baselineSourceOne );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .timeScale( scaleThreeWrapped )
                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( left )
                                                                       .right( right )
                                                                       .baseline( baseline )
                                                                       .build();

        Set<TimeScale> actual = DeclarationUtilities.getSourceTimeScales( evaluation );
        Set<TimeScale> expected = Set.of( scaleOne, scaleTwo, scaleThree );
        assertEquals( expected, actual );
    }

    @Test
    void testGetEarliestAnalysisDuration()
    {
        AnalysisTimes analysisTimes = AnalysisTimesBuilder.builder()
                                                          .minimum( Duration.ZERO )
                                                          .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .analysisTimes( analysisTimes )
                                                                       .build();
        assertEquals( Duration.ZERO, DeclarationUtilities.getEarliestAnalysisDuration( evaluation ) );
    }

    @Test
    void testGetLatestAnalysisDuration()
    {
        AnalysisTimes analysisTimes = AnalysisTimesBuilder.builder()
                                                          .maximum( Duration.ZERO )
                                                          .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .analysisTimes( analysisTimes )
                                                                       .build();
        assertEquals( Duration.ZERO, DeclarationUtilities.getLatestAnalysisDuration( evaluation ) );
    }

    @Test
    void testGetStartOfSeason()
    {
        MonthDay startOfSeason = MonthDay.of( 1, 2 );
        wres.config.yaml.components.Season season = SeasonBuilder.builder()
                                                                 .minimum( startOfSeason )
                                                                 .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .season( season )
                                                                       .build();
        assertEquals( startOfSeason, DeclarationUtilities.getStartOfSeason( evaluation ) );
    }

    @Test
    void testGetEndOfSeason()
    {
        MonthDay endOfSeason = MonthDay.of( 1, 2 );
        wres.config.yaml.components.Season season = SeasonBuilder.builder()
                                                                 .maximum( endOfSeason )
                                                                 .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .season( season )
                                                                       .build();
        assertEquals( endOfSeason, DeclarationUtilities.getEndOfSeason( evaluation ) );
    }

    @Test
    void testHasProbabilityThresholds()
    {
        Threshold probability = Threshold.newBuilder()
                                         .build();
        wres.config.yaml.components.Threshold wrapped = ThresholdBuilder.builder()
                                                                        .threshold( probability )
                                                                        .type( ThresholdType.PROBABILITY )
                                                                        .build();
        EvaluationDeclaration topLevelEvaluation =
                EvaluationDeclarationBuilder.builder()
                                            .probabilityThresholds( Set.of( wrapped ) )
                                            .build();
        EvaluationDeclaration thresholdSetEvaluation = EvaluationDeclarationBuilder.builder()
                                                                                   .thresholdSets( Set.of( wrapped ) )
                                                                                   .build();
        ThresholdSource thresholdSource = ThresholdSourceBuilder.builder()
                                                                .type( ThresholdType.PROBABILITY )
                                                                .build();
        EvaluationDeclaration serviceEvaluation =
                EvaluationDeclarationBuilder.builder()
                                            .thresholdSources( Set.of( thresholdSource ) )
                                            .build();
        Metric metric = MetricBuilder.builder()
                                     .parameters( MetricParametersBuilder.builder()
                                                                         .probabilityThresholds( Set.of( wrapped ) )
                                                                         .build() )
                                     .build();
        EvaluationDeclaration metricEvaluation = EvaluationDeclarationBuilder.builder()
                                                                             .metrics( Set.of( metric ) )
                                                                             .build();
        EvaluationDeclaration emptyEvaluation = EvaluationDeclarationBuilder.builder()
                                                                            .build();
        assertAll( () -> assertTrue( DeclarationUtilities.hasProbabilityThresholds( topLevelEvaluation ) ),
                   () -> assertTrue( DeclarationUtilities.hasProbabilityThresholds( thresholdSetEvaluation ) ),
                   () -> assertTrue( DeclarationUtilities.hasProbabilityThresholds( serviceEvaluation ) ),
                   () -> assertTrue( DeclarationUtilities.hasProbabilityThresholds( metricEvaluation ) ),
                   () -> assertFalse( DeclarationUtilities.hasProbabilityThresholds( emptyEvaluation ) ) );
    }

    @Test
    void testHasGeneratedBaseline()
    {
        Dataset dataset = DatasetBuilder.builder()
                                        .build();
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .build();
        BaselineDataset baselineDataset = BaselineDatasetBuilder.builder().dataset( dataset )
                                                                .generatedBaseline( persistence )
                                                                .build();
        BaselineDataset anotherBaselineDataset = BaselineDatasetBuilder.builder()
                                                                       .dataset( dataset )
                                                                       .build();
        assertAll( () -> assertTrue( DeclarationUtilities.hasGeneratedBaseline( baselineDataset ) ),
                   () -> assertFalse( DeclarationUtilities.hasGeneratedBaseline( anotherBaselineDataset ) ) );
    }

    @Test
    void testGetVariableName()
    {
        Dataset left = DatasetBuilder.builder()
                                     .variable( VariableBuilder.builder()
                                                               .name( "foo" )
                                                               .build() )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .variable( VariableBuilder.builder()
                                                                .name( "bar" )
                                                                .build() )
                                      .build();
        Dataset baseline = DatasetBuilder.builder()
                                         .variable( VariableBuilder.builder()
                                                                   .name( "baz" )
                                                                   .build() )
                                         .build();
        assertAll( () -> assertEquals( "foo", DeclarationUtilities.getVariableName( left ) ),
                   () -> assertEquals( "bar", DeclarationUtilities.getVariableName( right ) ),
                   () -> assertEquals( "baz", DeclarationUtilities.getVariableName( baseline ) ) );
    }

    @Test
    void testIsForecast()
    {
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.ENSEMBLE_FORECASTS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();
        Dataset baseline = DatasetBuilder.builder()
                                         .type( DataType.OBSERVATIONS )
                                         .build();
        assertAll( () -> assertTrue( DeclarationUtilities.isForecast( left ) ),
                   () -> assertTrue( DeclarationUtilities.isForecast( right ) ),
                   () -> assertFalse( DeclarationUtilities.isForecast( baseline ) ) );
    }

    @Test
    void testGetReferenceTimeType()
    {
        assertAll( () -> assertEquals( ReferenceTime.ReferenceTimeType.ANALYSIS_START_TIME,
                                       DeclarationUtilities.getReferenceTimeType( DataType.OBSERVATIONS ) ),
                   () -> assertEquals( ReferenceTime.ReferenceTimeType.ANALYSIS_START_TIME,
                                       DeclarationUtilities.getReferenceTimeType( DataType.ANALYSES ) ),
                   () -> assertEquals( ReferenceTime.ReferenceTimeType.T0,
                                       DeclarationUtilities.getReferenceTimeType( DataType.ENSEMBLE_FORECASTS ) ),
                   () -> assertEquals( ReferenceTime.ReferenceTimeType.T0,
                                       DeclarationUtilities.getReferenceTimeType( DataType.SINGLE_VALUED_FORECASTS ) ) );
    }

    @Test
    void testGetThresholds()
    {
        Threshold one = Threshold.newBuilder()
                                 .setLeftThresholdValue( 1.0 )
                                 .build();
        wres.config.yaml.components.Threshold wrappedOne = ThresholdBuilder.builder()
                                                                           .threshold( one )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        Threshold two = Threshold.newBuilder()
                                 .setLeftThresholdValue( 2.0 )
                                 .build();
        wres.config.yaml.components.Threshold wrappedTwo = ThresholdBuilder.builder()
                                                                           .threshold( two )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        Threshold three = Threshold.newBuilder()
                                   .setLeftThresholdValue( 3.0 )
                                   .build();
        wres.config.yaml.components.Threshold wrappedThree = ThresholdBuilder.builder()
                                                                             .threshold( three )
                                                                             .type( ThresholdType.VALUE )
                                                                             .build();

        Metric metric = MetricBuilder.builder()
                                     .parameters( MetricParametersBuilder.builder()
                                                                         .thresholds( Set.of( wrappedThree ) )
                                                                         .build() )
                                     .build();

        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .thresholds( Set.of( wrappedOne ) )
                                            .thresholdSets( Set.of( wrappedTwo ) )
                                            .metrics( Set.of( metric ) )
                                            .build();

        Set<wres.config.yaml.components.Threshold> actual = DeclarationUtilities.getThresholds( evaluation );
        Set<wres.config.yaml.components.Threshold> expected = Set.of( wrappedOne, wrappedTwo, wrappedThree );

        assertEquals( expected, actual );
    }

    @Test
    void testHasMissingDataTypes()
    {
        Dataset missing = DatasetBuilder.builder()
                                        .build();
        Dataset present = DatasetBuilder.builder()
                                        .type( DataType.OBSERVATIONS )
                                        .build();

        EvaluationDeclaration withAllMissing
                = EvaluationDeclarationBuilder.builder()
                                              .left( missing )
                                              .right( missing )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( missing )
                                                                               .build() )
                                              .build();

        EvaluationDeclaration withOneMissing
                = EvaluationDeclarationBuilder.builder()
                                              .left( missing )
                                              .right( present )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( missing )
                                                                               .build() )
                                              .build();

        EvaluationDeclaration withNoneMissing
                = EvaluationDeclarationBuilder.builder()
                                              .left( present )
                                              .right( present )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( present )
                                                                               .build() )
                                              .build();

        assertAll( () -> assertTrue( DeclarationUtilities.hasMissingDataTypes( withOneMissing ) ),
                   () -> assertTrue( DeclarationUtilities.hasMissingDataTypes( withAllMissing ) ),
                   () -> assertFalse( DeclarationUtilities.hasMissingDataTypes( withNoneMissing ) ) );

    }

    @Test
    void testAddThresholdsToDeclaration()
    {
        Metric metricOne = MetricBuilder.builder()
                                        .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                        .build();

        Metric metricTwo = MetricBuilder.builder()
                                        .name( MetricConstants.PROBABILITY_OF_DETECTION )
                                        .build();

        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .metrics( Set.of( metricOne, metricTwo ) )
                                            .build();

        Threshold one = Threshold.newBuilder()
                                 .setLeftThresholdValue( 0.1 )
                                 .build();
        wres.config.yaml.components.Threshold wrappedOne = ThresholdBuilder.builder()
                                                                           .threshold( one )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        Threshold two = Threshold.newBuilder()
                                 .setLeftThresholdValue( 0.2 )
                                 .build();
        wres.config.yaml.components.Threshold wrappedTwo = ThresholdBuilder.builder()
                                                                           .threshold( two )
                                                                           .type( ThresholdType.PROBABILITY )
                                                                           .build();
        Threshold three = Threshold.newBuilder()
                                   .setLeftThresholdValue( 0.3 )
                                   .build();
        wres.config.yaml.components.Threshold wrappedThree = ThresholdBuilder.builder()
                                                                             .threshold( three )
                                                                             .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                                             .build();
        Set<wres.config.yaml.components.Threshold> thresholds = Set.of( wrappedOne, wrappedTwo, wrappedThree );
        EvaluationDeclaration actual = DeclarationUtilities.addThresholds( evaluation, thresholds );

        Metric expectedMetricOne =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .probabilityThresholds( Set.of( wrappedTwo ) )
                                                                 .thresholds( Set.of( wrappedOne ) )
                                                                 .build() )
                             .build();

        Metric expectedMetricTwo =
                MetricBuilder.builder()
                             .name( MetricConstants.PROBABILITY_OF_DETECTION )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .probabilityThresholds( Set.of( wrappedTwo ) )
                                                                 .thresholds( Set.of( wrappedOne ) )
                                                                 .classifierThresholds( Set.of( wrappedThree ) )
                                                                 .build() )
                             .build();
        Set<Metric> expectedMetrics = Set.of( expectedMetricOne, expectedMetricTwo );
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .thresholds( Set.of( wrappedOne ) )
                                                                     .probabilityThresholds( Set.of( wrappedTwo ) )
                                                                     .classifierThresholds( Set.of( wrappedThree ) )
                                                                     .metrics( expectedMetrics )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testRemoveFeaturesWithoutThresholds()
    {
        Geometry left = Geometry.newBuilder()
                                .setName( "foo" )
                                .build();
        Geometry right = Geometry.newBuilder()
                                 .setName( "bar" )
                                 .build();
        Geometry baseline = Geometry.newBuilder()
                                    .setName( "baz" )
                                    .build();

        // Tuple foo-bar-baz
        GeometryTuple one = GeometryTuple.newBuilder()
                                         .setLeft( left )
                                         .setRight( right )
                                         .setBaseline( baseline )
                                         .build();
        // Tuple baz-foo-bar
        GeometryTuple two = GeometryTuple.newBuilder()
                                         .setLeft( baseline )
                                         .setRight( left )
                                         .setBaseline( right )
                                         .build();
        // Tuple bar-baz-foo
        GeometryTuple three = GeometryTuple.newBuilder()
                                           .setLeft( right )
                                           .setRight( baseline )
                                           .setBaseline( left )
                                           .build();

        Threshold threshold = Threshold.newBuilder()
                                       .setLeftThresholdValue( 1 )
                                       .build();
        wres.config.yaml.components.Threshold wrappedThresholdOne =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( left )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();
        wres.config.yaml.components.Threshold wrappedThresholdTwo =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( right )
                                .featureNameFrom( DatasetOrientation.RIGHT )
                                .build();
        wres.config.yaml.components.Threshold wrappedThresholdThree =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( baseline )
                                .featureNameFrom( DatasetOrientation.BASELINE )
                                .build();

        Set<GeometryTuple> geometryTuples = Set.of( one, two, three );
        Features features = new Features( geometryTuples );
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( geometryTuples )
                                           .setRegionName( "foorbarbaz" )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( group ) )
                                                          .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .features( features )
                                            .featureGroups( featureGroups )
                                            .thresholds( Set.of( wrappedThresholdOne,
                                                                 wrappedThresholdTwo,
                                                                 wrappedThresholdThree ) )
                                            .build();

        EvaluationDeclaration actual = DeclarationUtilities.removeFeaturesWithoutThresholds( declaration );

        Features expectedFeatures = new Features( Set.of( one ) );
        GeometryGroup expectedGroup = GeometryGroup.newBuilder()
                                                   .addGeometryTuples( one )
                                                   .setRegionName( "foorbarbaz" )
                                                   .build();
        FeatureGroups expectedFeatureGroups = FeatureGroupsBuilder.builder()
                                                                  .geometryGroups( Set.of( expectedGroup ) )
                                                                  .build();
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .features( expectedFeatures )
                                                                     .featureGroups( expectedFeatureGroups )
                                                                     .thresholds( Set.of( wrappedThresholdOne,
                                                                                          wrappedThresholdTwo,
                                                                                          wrappedThresholdThree ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testRemoveFeaturesWithoutThresholdsWhenThresholdsContainSingleDataOrientation()
    {
        Geometry left = Geometry.newBuilder()
                                .setName( "foo" )
                                .build();
        Geometry right = Geometry.newBuilder()
                                 .setName( "bar" )
                                 .build();
        Geometry baseline = Geometry.newBuilder()
                                    .setName( "baz" )
                                    .build();

        // Tuple foo-bar-baz
        GeometryTuple one = GeometryTuple.newBuilder()
                                         .setLeft( left )
                                         .setRight( right )
                                         .setBaseline( baseline )
                                         .build();

        Threshold threshold = Threshold.newBuilder()
                                       .setLeftThresholdValue( 1 )
                                       .build();
        wres.config.yaml.components.Threshold wrappedThresholdOne =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( left )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();

        Set<GeometryTuple> geometryTuples = Set.of( one );
        Features features = new Features( geometryTuples );
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( geometryTuples )
                                           .setRegionName( "foorbarbaz" )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( group ) )
                                                          .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .features( features )
                                            .featureGroups( featureGroups )
                                            .thresholds( Set.of( wrappedThresholdOne ) )
                                            .build();

        EvaluationDeclaration actual = DeclarationUtilities.removeFeaturesWithoutThresholds( declaration );

        Features expectedFeatures = new Features( Set.of( one ) );
        GeometryGroup expectedGroup = GeometryGroup.newBuilder()
                                                   .addGeometryTuples( one )
                                                   .setRegionName( "foorbarbaz" )
                                                   .build();
        FeatureGroups expectedFeatureGroups = FeatureGroupsBuilder.builder()
                                                                  .geometryGroups( Set.of( expectedGroup ) )
                                                                  .build();
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .features( expectedFeatures )
                                                                     .featureGroups( expectedFeatureGroups )
                                                                     .thresholds( Set.of( wrappedThresholdOne ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testHasFeatureGroups()
    {
        Geometry left = Geometry.newBuilder()
                                .setName( "foo" )
                                .build();
        Geometry right = Geometry.newBuilder()
                                 .setName( "bar" )
                                 .build();
        Geometry baseline = Geometry.newBuilder()
                                    .setName( "baz" )
                                    .build();

        GeometryTuple one = GeometryTuple.newBuilder()
                                         .setLeft( left )
                                         .setRight( right )
                                         .setBaseline( baseline )
                                         .build();

        Set<GeometryTuple> geometryTuples = Set.of( one );
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( geometryTuples )
                                           .setRegionName( "foorbarbaz" )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( group ) )
                                                          .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .featureGroups( featureGroups )
                                            .build();

        FeatureServiceGroup serviceGroup = new FeatureServiceGroup( "a", "b", true );
        FeatureService featureService = new FeatureService( URI.create( "http://foo.bar" ), Set.of( serviceGroup ) );
        EvaluationDeclaration declarationTwo =
                EvaluationDeclarationBuilder.builder()
                                            .featureService( featureService )
                                            .build();

        assertAll( () -> assertTrue( DeclarationUtilities.hasFeatureGroups( declaration ) ),
                   () -> assertTrue( DeclarationUtilities.hasFeatureGroups( declarationTwo ) ),
                   () -> assertFalse( DeclarationUtilities.hasFeatureGroups( EvaluationDeclarationBuilder.builder()
                                                                                                         .build() ) ) );
    }
}