package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.sampledata.pairs.MulticategoryPairs;
import wres.datamodel.statistics.MatrixOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContingencyTableTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * The metric to test.
     */

    private ContingencyTable<MulticategoryPairs> table;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
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
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata meta =
                MetricOutputMetadata.of( input.getRawData().size(),
                                                   MeasurementUnit.of(),
                                                   MeasurementUnit.of(),
                                                   MetricConstants.CONTINGENCY_TABLE,
                                                   MetricConstants.MAIN,
                                                   DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                         "SQIN",
                                                                                         "HEFS" ) );

        final double[][] benchmark = new double[][] { { 82.0, 38.0 }, { 23.0, 222.0 } };
        final MatrixOutput actual = table.apply( input );
        final MatrixOutput expected = MatrixOutput.of( benchmark,
                                                                  Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                                                 MetricDimension.FALSE_POSITIVES,
                                                                                 MetricDimension.FALSE_NEGATIVES,
                                                                                 MetricDimension.TRUE_NEGATIVES ),
                                                                  meta );
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
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'CONTINGENCY TABLE'." );
        table.apply( (DichotomousPairs) null );
    }

}
