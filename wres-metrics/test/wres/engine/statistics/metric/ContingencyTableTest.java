package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
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
        //Obtain the factories
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getMetadata(input.getData().size(),
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "Main",
                                                            null);

        //Build the metric
        final ContingencyTableBuilder<DichotomousPairs> b = new ContingencyTable.ContingencyTableBuilder<>();
        b.setOutputFactory(outF);
        final ContingencyTable<DichotomousPairs> table = b.build();
        final double[][] benchmark = new double[][]{{82.0, 38.0}, {23.0, 222.0}};
        final MatrixOutput actual = table.apply(input);
        final MatrixOutput expected = outF.ofMatrixOutput(benchmark, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue(table.getName().equals(metaFac.getMetricName(MetricConstants.CONTINGENCY_TABLE)));
    }

}
