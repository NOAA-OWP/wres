package wres.vis.data;

import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;

/**
 * Tests the {@link DataFactory}.
 * 
 * @author James Brown
 */

public class DataFactoryTest
{

    /**
     * Metadata to assist in testing.
     */

    private final PoolMetadata meta =
            PoolMetadata.of( PoolMetadata.of( Evaluation.newBuilder()
                                                            .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                                            .build(),
                                                  Pool.getDefaultInstance() ),
                               TimeWindowOuter.of( MessageFactory.getTimeWindow() ) );

    /**
     * Do not throw an IndexOutOfBoundsException when the input is empty. See #65503.
     */

    @Test( expected = Test.None.class /* no exception expected */ )
    public void testOfBoxPlotOutputDoesNotThrowIOOBExceptionWhenInputIsEmpty()
    {
        BoxplotStatisticOuter input = BoxplotStatisticOuter.of( BoxplotStatistic.newBuilder()
                                                                                .setMetric( BoxplotMetric.newBuilder()
                                                                                                         .setName( MetricName.BOX_PLOT_OF_ERRORS ) )
                                                                                .build(),
                                                                this.meta );

        DataFactory.ofBoxPlotOutput( 0, List.of( input ), null, ChronoUnit.SECONDS );
    }

}
