package wres.io.writing.commaseparated;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.StringJoiner;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * Tests the {@link CommaSeparatedUtilities}.
 */

public class CommaSeparatedUtilitiesTest
{
    private static final Evaluation EVALUATION = Evaluation.newBuilder()
                                                           .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                                           .build();;

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
        Pool pool = MessageFactory.getPool( (FeatureGroup) null,
                                          this.timeWindow,
                                          TimeScaleOuter.of(),
                                          null,
                                          false );

        PoolMetadata metadata = PoolMetadata.of( CommaSeparatedUtilitiesTest.EVALUATION, pool );

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

        Pool pool = MessageFactory.getPool( (FeatureGroup) null,
                                          this.timeWindow,
                                          timeScale,
                                          null,
                                          false );

        PoolMetadata metadata = PoolMetadata.of( CommaSeparatedUtilitiesTest.EVALUATION, pool );

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

}
