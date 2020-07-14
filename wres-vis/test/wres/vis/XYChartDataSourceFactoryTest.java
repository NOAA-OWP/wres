package wres.vis;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotStatistic;

/**
 * Tests the {@link XYChartDataSourceFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class XYChartDataSourceFactoryTest
{

    /**
     * Metadata to assist in testing.
     */

    private final StatisticMetadata meta =
            StatisticMetadata.of( SampleMetadata.of( SampleMetadata.of( MeasurementUnit.of() ), TimeWindowOuter.of() ),
                                  0,
                                  MeasurementUnit.of(),
                                  MetricConstants.BOX_PLOT_OF_ERRORS,
                                  MetricConstants.MAIN );

    /**
     * Do not throw an IndexOutOfBoundsException when the input is empty. See #65503.
     */

    @Test( expected = Test.None.class /* no exception expected */ )
    public void testOfBoxPlotOutputDoesNotThrowIOOBExceptionWhenInputIsEmpty()
    {
        BoxplotStatisticOuter input = BoxplotStatisticOuter.of( BoxplotStatistic.getDefaultInstance(), this.meta );

        XYChartDataSourceFactory.ofBoxPlotOutput( 0, List.of( input ), null, ChronoUnit.SECONDS );
    }


}
