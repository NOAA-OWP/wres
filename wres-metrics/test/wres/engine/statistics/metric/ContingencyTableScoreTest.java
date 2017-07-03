package wres.engine.statistics.metric;

import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;

/**
 * Tests the {@link ContingencyTableScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ContingencyTableScoreTest
{

    /**
     * Constructs a {@link ContingencyTableScore} and tests it.
     */

    @Test
    public void test1ContingencyTableScore()
    {
        final MetricOutputFactory outputFactory = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = outputFactory.getMetadataFactory();
        final MetricFactory metricFactory = MetricFactory.getInstance(outputFactory);
        final CriticalSuccessIndex cs = metricFactory.ofCriticalSuccessIndex();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getMetadata(365,
                                                            metaFac.getDimension(),
                                                            MetricConstants.CONTINGENCY_TABLE,
                                                            MetricConstants.MAIN,
                                                            "Main",
                                                            null);

        final double[][] benchmark = new double[][]{{82.0, 38.0}, {23.0, 222.0}};
        final MatrixOutput expected = outputFactory.ofMatrixOutput(benchmark, m1);

        //Check the exceptions
        try
        {
            cs.is2x2ContingencyTable(expected, cs);
        }
        catch(final Exception e)
        {
            fail("Expected a 2x2 contingency table: " + e.getMessage());
        }
        try
        {
            cs.is2x2ContingencyTable(outputFactory.ofMatrixOutput(new double[][]{{1.0}}, m1), cs);
            fail("Expected an exception on construction with an incorrectly sized matrix.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            cs.is2x2ContingencyTable(outputFactory.ofMatrixOutput(new double[][]{{1.0, 1.0, 1.0}, {1.0, 1.0, 1.0}}, m1),
                                     cs);
            fail("Expected an exception on construction with a non-square matrix.");
        }
        catch(final Exception e)
        {
        }
    }

}
