package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Boilerplate;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricTestDataFactory;

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
        table = ContingencyTable.of();
    }

    /**
     * Compares the output from {@link Metric#apply(wres.datamodel.sampledata.MetricInput)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        final SampleData<Pair<Boolean,Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final StatisticMetadata meta =
                StatisticMetadata.of( Boilerplate.getSampleMetadata(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.CONTINGENCY_TABLE,
                                      null );

        Map<MetricConstants, Double> expectedElements = new HashMap<>();
        expectedElements.put( MetricConstants.TRUE_POSITIVES, 82.0 );
        expectedElements.put( MetricConstants.TRUE_NEGATIVES, 222.0 );
        expectedElements.put( MetricConstants.FALSE_POSITIVES, 38.0 );
        expectedElements.put( MetricConstants.FALSE_NEGATIVES, 23.0 );

        final DoubleScoreStatistic actual = table.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( expectedElements, meta );
        assertTrue( "Unexpected result for the contingency table.", actual.equals( expected ) );

        //Check the parameters
        assertTrue( table.getName().equals( MetricConstants.CONTINGENCY_TABLE.toString() ) );
    }

    /**
     * Checks that the {@link ContingencyTable#getName()} returns {@link MetricConstants.CONTINGENCY_TABLE.toString()} 
     */

    @Test
    public void testContingencyTableIsNamedCorrectly()
    {
        assertTrue( table.getName().equals( MetricConstants.CONTINGENCY_TABLE.toString() ) );
    }

    /**
     * Checks for an expected exception on null input to {@link ContingencyTable#apply(wres.datamodel.sampledata.MetricInput)}.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> table.apply( (SampleData<Pair<Boolean, Boolean>>) null ) );

        String expectedMessage = "Specify non-null input to the 'CONTINGENCY TABLE'.";

        assertEquals( expectedMessage, exception.getMessage() );
    }

}
