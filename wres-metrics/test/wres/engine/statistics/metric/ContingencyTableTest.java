package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.engine.statistics.metric.ContingencyTable.ContingencyTableBuilder;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;

/**
 * <p>
 * Tests the {@link ContingencyTable}.
 * </p>
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

        //Build the metric
        final ContingencyTableBuilder<DichotomousPairs, MatrixOutput> b =
                                                                        new ContingencyTable.ContingencyTableBuilder<>();

        final ContingencyTable<DichotomousPairs, MatrixOutput> table = b.build();

        final double[][] benchmark = new double[][]{{82.0, 38.0}, {23.0, 222.0}};
        final MatrixOutput actual = table.apply(input);
        final MatrixOutput expected = MetricOutputFactory.ofMatrixOutput(benchmark, 365, null);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue(table.getName().equals("Contingency Table"));
        //Check the exceptions
        try
        {
            table.is2x2ContingencyTable(actual, table);
        }
        catch(final Exception e)
        {
            fail("Expected a 2x2 contingency table.");
        }
        try
        {
            table.is2x2ContingencyTable(MetricOutputFactory.ofScalarOutput(1, 1, null), table);
            fail("Expected a 2x2 contingency table.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            table.is2x2ContingencyTable(MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0}}, 1, null), table);
            fail("Expected a 2x2 contingency table.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            table.is2x2ContingencyTable(MetricOutputFactory.ofMatrixOutput(new double[][]{{1.0, 1.0, 1.0},
                {1.0, 1.0, 1.0}}, 1, null), table);
            fail("Expected a 2x2 contingency table.");
        }
        catch(final Exception e)
        {
        }
    }

}
