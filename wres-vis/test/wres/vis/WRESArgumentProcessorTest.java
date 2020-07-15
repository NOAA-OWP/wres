package wres.vis;

import static org.junit.Assert.assertEquals;

import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.Test;

import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link WresArgumentProcessor}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class WRESArgumentProcessorTest
{

    /**
     * Metadata to assist in testing.
     */

    private final SampleMetadata meta = SampleMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                           TimeWindowOuter.of() );

    /**
     * Do not throw an IndexOutOfBoundsException when the input is empty. See #65503.
     */

    @Test( expected = Test.None.class /* no exception expected */ )
    public void testConstructionDoesNotThrowIOOBExceptionWhenInputIsEmpty()
    {
        new WRESArgumentProcessor( BoxplotStatisticOuter.of( BoxplotStatistic.newBuilder()
                                                                             .setMetric( BoxplotMetric.newBuilder()
                                                                                                      .setName( MetricName.BOX_PLOT_OF_ERRORS ) )
                                                                             .build(),
                                                             this.meta ),
                                   ChronoUnit.SECONDS );
    }

    /**
     * Checks for the expected representation of probabilities when the input is empty. See #65503.
     */

    @Test
    public void testConstructionProducesExpectedProbabilitiesWhenInputIsEmpty()
    {
        WRESArgumentProcessor processor =
                new WRESArgumentProcessor( BoxplotStatisticOuter.of( BoxplotStatistic.newBuilder()
                                                                                     .setMetric( BoxplotMetric.newBuilder()
                                                                                                              .setName( MetricName.BOX_PLOT_OF_ERRORS ) )
                                                                                     .build(),
                                                                     this.meta ),
                                           ChronoUnit.SECONDS );

        String probs = processor.getArgument( "probabilities" ).getValue();

        assertEquals( "none defined", probs );
    }

    /**
     * Checks for the expected representation of probabilities when the input is not empty. See #65503.
     */

    @Test
    public void testConstructionProducesExpectedProbabilitiesWhenInputIsFull()
    {
        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( 1.0, 2.0, 3.0, 4.0, 5.0 ) )
                     .build();

        BoxplotStatistic aBox = BoxplotStatistic.newBuilder()
                                                .setMetric( metric )
                                                .addStatistics( box )
                                                .build();

        WRESArgumentProcessor processor =
                new WRESArgumentProcessor( BoxplotStatisticOuter.of( aBox, this.meta ),
                                           ChronoUnit.SECONDS );

        String probs = processor.getArgument( "probabilities" ).getValue();

        assertEquals( "min, 0.25, 0.5, 0.75, max", probs );
    }

}
