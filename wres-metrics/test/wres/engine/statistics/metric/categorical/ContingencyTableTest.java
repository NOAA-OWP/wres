package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContingencyTableTest
{

    /**
     * The metric to test.
     */

    private ContingencyTable table;

    @Before
    public void setupBeforeEachTest()
    {
        this.table = ContingencyTable.of();
    }

    /**
     * Compares the output from {@link Metric#apply(wres.datamodel.sampledata.MetricInput)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                               "SQIN",
                                                                               "HEFS" ) );

        DoubleScoreStatistic result =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_POSITIVES )
                                                                                 .setValue( 82.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_POSITIVES )
                                                                                 .setValue( 38.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_NEGATIVES )
                                                                                 .setValue( 23.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_NEGATIVES )
                                                                                 .setValue( 222.0 ) )
                                    .build();

        final DoubleScoreStatisticOuter actual = this.table.apply( input );
        final DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( result, meta );
        assertTrue( "Unexpected result for the contingency table.", actual.equals( expected ) );

        //Check the parameters
        assertTrue( this.table.getName().equals( MetricConstants.CONTINGENCY_TABLE.toString() ) );
    }

    /**
     * Checks that the {@link ContingencyTable#getName()} returns {@link MetricConstants.CONTINGENCY_TABLE.toString()} 
     */

    @Test
    public void testContingencyTableIsNamedCorrectly()
    {
        assertTrue( this.table.getName().equals( MetricConstants.CONTINGENCY_TABLE.toString() ) );
    }

    /**
     * Checks for an expected exception on null input to {@link ContingencyTable#apply(wres.datamodel.sampledata.MetricInput)}.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> this.table.apply( (SampleData<Pair<Boolean, Boolean>>) null ) );

        String expectedMessage = "Specify non-null input to the 'CONTINGENCY TABLE'.";

        assertEquals( expectedMessage, exception.getMessage() );
    }

}
