package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;

/**
 * Tests the {@link MetricInputFactory}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricInputFactoryTest
{

    /**
     * Tests the methods in {@link MetricInputFactory}.
     */

    @Test
    public void test1MetricFactory()
    {
        final Metadata m1 = MetadataFactory.getMetadata(1,
                                                        MetadataFactory.getDimension(),
                                                        "Main");       
        final DataFactory d = DataFactory.instance();
        final List<VectorOfBooleans> input = new ArrayList<>();
        input.add(d.vectorOf(new boolean[]{true,false}));
        MetricInputFactory.ofDichotomousPairs(input,m1);
        MetricInputFactory.ofMulticategoryPairs(input,m1);
        
        final List<PairOfDoubles> dInput = new ArrayList<>();
        dInput.add(d.pairOf(0.0,1.0)); 
        final Metadata m2 = MetadataFactory.getMetadata(dInput.size(),
                                                        MetadataFactory.getDimension(),
                                                        "Main");
        final Metadata m3 = MetadataFactory.getMetadata(dInput.size(),
                                                          MetadataFactory.getDimension(),
                                                          "Baseline");
        MetricInputFactory.ofDiscreteProbabilityPairs(dInput,m2);        
        MetricInputFactory.ofDiscreteProbabilityPairs(dInput, dInput,m2,m3);
        MetricInputFactory.ofExtendsSingleValuedPairs(dInput,m3);
        MetricInputFactory.ofSingleValuedPairs(dInput,m3);
        MetricInputFactory.ofSingleValuedPairs(dInput,dInput,m2,m3);
    }

}
