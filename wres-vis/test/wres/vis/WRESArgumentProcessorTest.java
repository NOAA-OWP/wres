package wres.vis;

import static org.junit.Assert.assertEquals;

import java.time.temporal.ChronoUnit;
import java.util.Collections;

import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeWindowOuter;

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
    public void testConstructionDoesNotThrowIOOBExceptionWhenInputIsEmpty()
    {
        new WRESArgumentProcessor( BoxPlotStatistics.of( Collections.emptyList(), this.meta ), ChronoUnit.SECONDS );
    }

    /**
     * Checks for the expected representation of probabilities when the input is empty. See #65503.
     */

    @Test
    public void testConstructionProducesExpectedProbabilitiesWhenInputIsEmpty()
    {
        WRESArgumentProcessor processor =
                new WRESArgumentProcessor( BoxPlotStatistics.of( Collections.emptyList(), this.meta ),
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
        BoxPlotStatistic box =
                BoxPlotStatistic.of( VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 ),
                                     VectorOfDoubles.of( 1, 2, 3, 4, 5 ),
                                     meta );

        WRESArgumentProcessor processor =
                new WRESArgumentProcessor( BoxPlotStatistics.of( Collections.singletonList( box ), this.meta ),
                                           ChronoUnit.SECONDS );

        String probs = processor.getArgument( "probabilities" ).getValue();

        assertEquals( "min, 0.25, 0.5, 0.75, max", probs );
    }


}
