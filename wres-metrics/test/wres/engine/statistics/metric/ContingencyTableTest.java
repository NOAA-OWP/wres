package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.ContingencyTable.ContingencyTableBuilder;

/**
 * Tests the {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ContingencyTableTest
{

    /**
     * Constructs a {@link ContingencyTable} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1ContingencyTable()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();
        
        //Metadata for the output
        final MetricOutputMetadata m1 =
                                      MetadataFactory.getMetadata(input.getData().size(),
                                                                  MetadataFactory.getDimension(),
                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                  MetricConstants.MAIN,
                                                                  "Main",
                                                                  null);

        //Build the metric
        final ContingencyTableBuilder<DichotomousPairs, MatrixOutput> b =
                                                                        new ContingencyTable.ContingencyTableBuilder<>();

        final ContingencyTable<DichotomousPairs, MatrixOutput> table = b.build();
        final double[][] benchmark = new double[][]{{82.0, 38.0}, {23.0, 222.0}};
        final MatrixOutput actual = table.apply(input);
        final MatrixOutput expected = MetricOutputFactory.ofMatrixOutput(benchmark,m1);       
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue(table.getName().equals(MetadataFactory.getMetricName(MetricConstants.CONTINGENCY_TABLE)));
        //Check the exceptions
        try
        {
            table.is2x2ContingencyTable(actual, table);
        }
        catch(final Exception e)
        {
            fail("Expected a 2x2 contingency table: "+e.getMessage());
        }
        try
        {
            table.is2x2ContingencyTable(MetricOutputFactory.ofScalarOutput(1,m1), table);
            fail("Expected an exception on construction with a scalar output.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            table.is2x2ContingencyTable(MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}},m1), table);
            fail("Expected an exception on construction with an incorrectly sized matrix.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            table.is2x2ContingencyTable(MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0, 1.0, 1.0},
                {1.0, 1.0, 1.0}},m1), table);
            fail("Expected an exception on construction with a non-square matrix.");
        }
        catch(final Exception e)
        {
        }
    }

}
