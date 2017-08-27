package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.Metadata;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOutput;
import wres.engine.statistics.metric.RelativeOperatingCharacteristicScore.RelativeOperatingCharacteristicScoreBuilder;

/**
 * Tests the {@link RelativeOperatingCharacteristicScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class RelativeOperatingCharacteristicScoreTest
{

    /**
     * Constructs a {@link RelativeOperatingCharacteristicScore} and compares the actual result to the expected result
     * for the model with ties from Mason and Graham (2002).
     */

    @Test
    public void test1RelativeOperatingCharacteristicScoreWithTies()
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(outF.pairOf(0, 0.8));
        values.add(outF.pairOf(0, 0.8));
        values.add(outF.pairOf(0, 0.0));
        values.add(outF.pairOf(1, 1.0));
        values.add(outF.pairOf(1, 1.0));
        values.add(outF.pairOf(1, 0.6));
        values.add(outF.pairOf(0, 0.4));
        values.add(outF.pairOf(1, 0.8));
        values.add(outF.pairOf(1, 0.0));
        values.add(outF.pairOf(0, 0.0));
        values.add(outF.pairOf(0, 0.2));
        values.add(outF.pairOf(0, 0.0));
        values.add(outF.pairOf(0, 0.0));
        values.add(outF.pairOf(1, 1.0));
        values.add(outF.pairOf(1, 1.0));

        final DiscreteProbabilityPairs input = outF.ofDiscreteProbabilityPairs(values, metaFac.getMetadata());

        //Build the metric
        final RelativeOperatingCharacteristicScoreBuilder b = new RelativeOperatingCharacteristicScoreBuilder();
        b.setOutputFactory(outF);

        final RelativeOperatingCharacteristicScore rocs = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE,
                                                                  MetricConstants.NONE);

        //Check the results       
        final VectorOutput actual = rocs.apply(input);
        final VectorOutput expected = outF.ofVectorOutput(new double[]{0.6785714285714286}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the ROC Score.",
                   rocs.getName()
                       .equals(metaFac.getMetricName(MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE)));
        assertTrue("The ROC Score is not decomposable.", !rocs.isDecomposable());
        assertTrue("The ROC Score is a skill score.", rocs.isSkillScore());
        assertTrue("Expected no decomposition for the ROC Score.",
                   rocs.getDecompositionID() == MetricDecompositionGroup.NONE);
        assertTrue("The ROC Score is not proper.", !rocs.isProper());
        assertTrue("The ROC Score is not strictly proper.", !rocs.isStrictlyProper());
    }

    /**
     * Constructs a {@link RelativeOperatingCharacteristicScore} and compares the actual result to the expected result
     * for the model with ties from Mason and Graham (2002).
     */

    @Test
    public void test1RelativeOperatingCharacteristicScoreWithoutTies()
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(outF.pairOf(0, 0.928));
        values.add(outF.pairOf(0, 0.576));
        values.add(outF.pairOf(0, 0.008));
        values.add(outF.pairOf(1, 0.944));
        values.add(outF.pairOf(1, 0.832));
        values.add(outF.pairOf(1, 0.816));
        values.add(outF.pairOf(0, 0.136));
        values.add(outF.pairOf(1, 0.584));
        values.add(outF.pairOf(1, 0.032));
        values.add(outF.pairOf(0, 0.016));
        values.add(outF.pairOf(0, 0.28));
        values.add(outF.pairOf(0, 0.024));
        values.add(outF.pairOf(0, 0.0));
        values.add(outF.pairOf(1, 0.984));
        values.add(outF.pairOf(1, 0.952));
        Metadata meta = metaFac.getMetadata();
        final DiscreteProbabilityPairs input = outF.ofDiscreteProbabilityPairs(values, meta);

        //Build the metric
        final RelativeOperatingCharacteristicScoreBuilder b = new RelativeOperatingCharacteristicScoreBuilder();
        b.setOutputFactory(outF);

        final RelativeOperatingCharacteristicScore rocs = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE,
                                                                  MetricConstants.NONE);

        //Check the results       
        final VectorOutput actual = rocs.apply(input);
        final VectorOutput expected = outF.ofVectorOutput(new double[]{0.75}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));
        //Check against a baseline
        final DiscreteProbabilityPairs inputBase = outF.ofDiscreteProbabilityPairs(values, values, meta, meta);
        final VectorOutput actualBase = rocs.apply(inputBase);
        final VectorOutput expectedBase = outF.ofVectorOutput(new double[]{0.0}, m1);
        assertTrue("Actual: " + actualBase.getData().getDoubles()[0] + ". Expected: "
            + expectedBase.getData().getDoubles()[0] + ".", actualBase.equals(expectedBase));
    }
    
    /**
     * Constructs a {@link RelativeOperatingCharacteristicScore} and compares the actual result to the expected result
     * for an example dataset with no occurrences.
     */

    @Test
    public void test1RelativeOperatingCharacteristicScoreNoOccurrences()
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(outF.pairOf(0, 0.928));
        values.add(outF.pairOf(0, 0.576));
        values.add(outF.pairOf(0, 0.008));
        values.add(outF.pairOf(0, 0.944));
        values.add(outF.pairOf(0, 0.832));
        values.add(outF.pairOf(0, 0.816));
        values.add(outF.pairOf(0, 0.136));
        values.add(outF.pairOf(0, 0.584));
        values.add(outF.pairOf(0, 0.032));
        values.add(outF.pairOf(0, 0.016));
        values.add(outF.pairOf(0, 0.28));
        values.add(outF.pairOf(0, 0.024));
        values.add(outF.pairOf(0, 0.0));
        values.add(outF.pairOf(0, 0.984));
        values.add(outF.pairOf(0, 0.952));
        Metadata meta = metaFac.getMetadata();
        final DiscreteProbabilityPairs input = outF.ofDiscreteProbabilityPairs(values, meta);

        //Build the metric
        final RelativeOperatingCharacteristicScoreBuilder b = new RelativeOperatingCharacteristicScoreBuilder();
        b.setOutputFactory(outF);

        final RelativeOperatingCharacteristicScore rocs = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE,
                                                                  MetricConstants.NONE);

        //Check the results       
        final VectorOutput actual = rocs.apply(input);
        final VectorOutput expected = outF.ofVectorOutput(new double[]{Double.NaN}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));
    }    
    

}
