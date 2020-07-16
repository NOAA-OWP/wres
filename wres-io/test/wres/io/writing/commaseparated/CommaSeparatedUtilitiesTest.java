package wres.io.writing.commaseparated;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.StringJoiner;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.Builder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.time.TimeWindowOuter;

/**
 * Tests the {@link CommaSeparatedUtilities}.
 */

public class CommaSeparatedUtilitiesTest
{
    private static final FeatureKey FEATURE_ONE = FeatureKey.of( "fooBasin" );
    private static final FeatureKey FEATURE_TWO = FeatureKey.of( "fooBasiny" );
    private static final FeatureTuple FEATURE_TUPLE = new FeatureTuple( FEATURE_TWO, FEATURE_ONE, FEATURE_TWO );

    private TimeWindowOuter timeWindow;

    @Before
    public void runBeforeEachTest()
    {
        Instant earlyRef = Instant.parse( "2017-12-31T00:00:00Z" );
        Instant lateRef = Instant.parse( "2017-12-31T12:00:00Z" );
        Instant earlyValid = Instant.parse( "2008-01-31T12:00:00Z" );
        Instant lateValid = Instant.parse( "2019-12-31T12:00:00Z" );
        Duration earlyLead = Duration.ofHours( 6 );
        Duration lateLead = Duration.ofHours( 120 );

        timeWindow = TimeWindowOuter.of( earlyRef, lateRef, earlyValid, lateValid, earlyLead, lateLead );
    }

    @Test
    public void testGetTimeWindowHeaderFromSampleMetadataWithInstantaneousTimeScale()
    {

        SampleMetadata metadata = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                                             .setTimeWindow( this.timeWindow )
                                                             .setTimeScale( TimeScaleOuter.of() )
                                                             .build();

        String expected = "EARLIEST ISSUE TIME,"
                          + "LATEST ISSUE TIME,"
                          + "EARLIEST VALID TIME,"
                          + "LATEST VALID TIME,"
                          + "EARLIEST LEAD TIME IN HOURS [INSTANTANEOUS],"
                          + "LATEST LEAD TIME IN HOURS [INSTANTANEOUS]";

        StringJoiner actual =
                CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( metadata, ChronoUnit.HOURS );

        assertEquals( expected, actual.toString() );
    }

    @Test
    public void testGetTimeWindowHeaderFromSampleMetadataWithAccumulatedTimeScale()
    {
        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofHours( 1 ), TimeScaleFunction.TOTAL );

        SampleMetadata metadata = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                                             .setTimeWindow( this.timeWindow )
                                                             .setTimeScale( timeScale )
                                                             .build();

        String expected = "EARLIEST ISSUE TIME,"
                          + "LATEST ISSUE TIME,"
                          + "EARLIEST VALID TIME,"
                          + "LATEST VALID TIME,"
                          + "EARLIEST LEAD TIME IN HOURS [TOTAL OVER PAST 1 HOURS],"
                          + "LATEST LEAD TIME IN HOURS [TOTAL OVER PAST 1 HOURS]";

        StringJoiner actual =
                CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( metadata, ChronoUnit.HOURS );

        assertEquals( expected, actual.toString() );
    }

    @Test
    public void testGetFeatureNameFromMetadataWithNamedLocation()
    {

        DatasetIdentifier identifier = DatasetIdentifier.of( FEATURE_TUPLE, "barVariable" );

        SampleMetadata metadata = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                                             .setIdentifier( identifier )
                                                             .build();

        String actual =
                CommaSeparatedUtilities.getFeatureNameFromMetadata( metadata );


        assertEquals( "fooBasin", actual.toString() );

    }

    @Test
    public void testGetFeatureNameFromMetadataWithGeographicLocation()
    {

        DatasetIdentifier identifier = DatasetIdentifier.of( FEATURE_TUPLE, "barVariable" );

        SampleMetadata metadata = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                                             .setIdentifier( identifier )
                                                             .build();

        String actual =
                CommaSeparatedUtilities.getFeatureNameFromMetadata( metadata );

        assertEquals( FEATURE_TUPLE.getRightName(), actual );

    }

    @Test
    public void testGetFeatureNameFromMetadataWithNullLocation()
    {

        DatasetIdentifier identifier = DatasetIdentifier.of( (FeatureTuple) null, "barVariable" );

        SampleMetadata metadata = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                                             .setIdentifier( identifier )
                                                             .build();

        String actual =
                CommaSeparatedUtilities.getFeatureNameFromMetadata( metadata );

        assertEquals( "UNKNOWN", actual.toString() );

    }

}
