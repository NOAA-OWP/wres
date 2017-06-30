package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.List;

import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.inputs.MetricInputFactory;
import wres.engine.statistics.metric.inputs.MulticategoryPairs;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricTestDataFactory
{

    /**
     * Returns a set of single-valued pairs without a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsOne()
    {
        //Construct some single-valued pairs
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory dataFactory = DataFactory.instance();
        values.add(dataFactory.pairOf(22.9, 22.8));
        values.add(dataFactory.pairOf(75.2, 80));
        values.add(dataFactory.pairOf(63.2, 65));
        values.add(dataFactory.pairOf(29, 30));
        values.add(dataFactory.pairOf(5, 2));
        values.add(dataFactory.pairOf(2.1, 3.1));
        values.add(dataFactory.pairOf(35000, 37000));
        values.add(dataFactory.pairOf(8, 7));
        values.add(dataFactory.pairOf(12, 12));
        values.add(dataFactory.pairOf(93, 94));
        return MetricInputFactory.ofSingleValuedPairs(values, MetadataFactory.getMetadata(values.size()));
    }

    /**
     * Returns a set of single-valued pairs with a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsTwo()
    {
        //Construct some single-valued pairs
        final DataFactory dataFactory = DataFactory.instance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(dataFactory.pairOf(22.9, 22.8));
        values.add(dataFactory.pairOf(75.2, 80));
        values.add(dataFactory.pairOf(63.2, 65));
        values.add(dataFactory.pairOf(29, 30));
        values.add(dataFactory.pairOf(5, 2));
        values.add(dataFactory.pairOf(2.1, 3.1));
        values.add(dataFactory.pairOf(35000, 37000));
        values.add(dataFactory.pairOf(8, 7));
        values.add(dataFactory.pairOf(12, 12));
        values.add(dataFactory.pairOf(93, 94));
        final List<PairOfDoubles> baseline = new ArrayList<>();
        baseline.add(dataFactory.pairOf(20.9, 23.8));
        baseline.add(dataFactory.pairOf(71.2, 83.2));
        baseline.add(dataFactory.pairOf(69.2, 66));
        baseline.add(dataFactory.pairOf(20, 30.5));
        baseline.add(dataFactory.pairOf(5.8, 2.1));
        baseline.add(dataFactory.pairOf(1.1, 3.4));
        baseline.add(dataFactory.pairOf(33020, 37500));
        baseline.add(dataFactory.pairOf(8.8, 7.1));
        baseline.add(dataFactory.pairOf(12.1, 13));
        baseline.add(dataFactory.pairOf(93.2, 94.8));
        final Metadata main = MetadataFactory.getMetadata(values.size(), MetadataFactory.getDimension("CMS"), "Main");
        final Metadata base =
                            MetadataFactory.getMetadata(values.size(), MetadataFactory.getDimension("CMS"), "Baseline");
        return MetricInputFactory.ofSingleValuedPairs(values, baseline, main, base);
    }

    /**
     * Returns a moderately-sized (10k) test dataset of single-valued pairs, {5,10}, without a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsThree()
    {
        //Construct some single-valued pairs
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory dataFactory = DataFactory.instance();
        for(int i = 0; i < 10000; i++)
        {
            values.add(dataFactory.pairOf(5, 10));
        }
        final Metadata meta = MetadataFactory.getMetadata(values.size(), MetadataFactory.getDimension("CMS"), "Main");
        return MetricInputFactory.ofSingleValuedPairs(values, meta);
    }

    /**
     * Returns a set of dichotomous pairs based on http://www.cawcr.gov.au/projects/verification/#Contingency_table. The
     * test data comprises 83 hits, 38 false alarms, 23 misses and 222 correct negatives, i.e. N=365.
     * 
     * @return a set of dichotomous pairs
     */

    public static DichotomousPairs getDichotomousPairsOne()
    {

        final DataFactory d = DataFactory.instance();

        //Construct the dichotomous pairs using the example from http://www.cawcr.gov.au/projects/verification/#Contingency_table
        //83 hits, 38 false alarms, 23 misses and 222 correct negatives, i.e. N=365
        final List<VectorOfBooleans> values = new ArrayList<>();
        //Hits
        for(int i = 0; i < 82; i++)
        {
            values.add(d.vectorOf(new boolean[]{true, true}));
        }
        //False alarms
        for(int i = 82; i < 120; i++)
        {
            values.add(d.vectorOf(new boolean[]{false, true}));
        }
        //Misses
        for(int i = 120; i < 143; i++)
        {
            values.add(d.vectorOf(new boolean[]{true, false}));
        }
        for(int i = 144; i < 366; i++)
        {
            values.add(d.vectorOf(new boolean[]{false, false}));
        }
        final Metadata meta = MetadataFactory.getMetadata(values.size(), MetadataFactory.getDimension(), "Main");
        return MetricInputFactory.ofDichotomousPairs(values, meta); //Construct the pairs
    }

    /**
     * Returns a set of multicategory pairs based on Table 4.2 in Joliffe and Stephenson (2012) Forecast Verification: A
     * Practitioner's Guide in Atmospheric Science. 2nd Ed. Wiley, Chichester.
     * 
     * @return a set of dichotomous pairs
     */

    public static MulticategoryPairs getMulticategoryPairsOne()
    {

        final DataFactory d = DataFactory.instance();

        //Construct the multicategory pairs
        final List<VectorOfBooleans> values = new ArrayList<>();
        //(1,1)
        for(int i = 0; i < 24; i++)
        {
            values.add(d.vectorOf(new boolean[]{true, false, false, true, false, false}));
        }
        //(1,2)
        for(int i = 24; i < 87; i++)
        {
            values.add(d.vectorOf(new boolean[]{false, true, false, true, false, false}));
        }
        //(1,3)
        for(int i = 87; i < 118; i++)
        {
            values.add(d.vectorOf(new boolean[]{false, false, true, true, false, false}));
        }
        //(2,1)
        for(int i = 118; i < 181; i++)
        {
            values.add(d.vectorOf(new boolean[]{true, false, false, false, true, false}));
        }
        //(2,2)
        for(int i = 181; i < 284; i++)
        {
            values.add(d.vectorOf(new boolean[]{false, true, false, false, true, false}));
        }
        //(2,3)
        for(int i = 284; i < 426; i++)
        {
            values.add(d.vectorOf(new boolean[]{false, false, true, false, true, false}));
        }
        //(3,1)
        for(int i = 426; i < 481; i++)
        {
            values.add(d.vectorOf(new boolean[]{true, false, false, false, false, true}));
        }
        //(3,2)
        for(int i = 481; i < 591; i++)
        {
            values.add(d.vectorOf(new boolean[]{false, true, false, false, false, true}));
        }
        //(3,3)
        for(int i = 591; i < 788; i++)
        {
            values.add(d.vectorOf(new boolean[]{false, false, true, false, false, true}));
        }
        final Metadata meta = MetadataFactory.getMetadata(values.size(), MetadataFactory.getDimension(), "Main");
        return MetricInputFactory.ofMulticategoryPairs(values, meta); //Construct the pairs
    }

    /**
     * Returns a set of discrete probability pairs without a baseline.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsOne()
    {
        //Construct some probabilistic pairs, and use the same pairs as a reference for skill (i.e. skill = 0.0)
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory dataFactory = DataFactory.instance();
        values.add(dataFactory.pairOf(0, 3.0 / 5.0));
        values.add(dataFactory.pairOf(0, 1.0 / 5.0));
        values.add(dataFactory.pairOf(1, 2.0 / 5.0));
        values.add(dataFactory.pairOf(1, 3.0 / 5.0));
        values.add(dataFactory.pairOf(0, 0.0 / 5.0));
        values.add(dataFactory.pairOf(1, 1.0 / 5.0));
        final Metadata meta = MetadataFactory.getMetadata(values.size(), MetadataFactory.getDimension(), "Main");
        return MetricInputFactory.ofDiscreteProbabilityPairs(values, meta);
    }

    /**
     * Returns a set of discrete probability pairs with a baseline.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsTwo()
    {
        //Construct some probabilistic pairs, and use some different pairs as a reference
        final DataFactory dataFactory = DataFactory.instance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(dataFactory.pairOf(0, 3.0 / 5.0));
        values.add(dataFactory.pairOf(0, 1.0 / 5.0));
        values.add(dataFactory.pairOf(1, 2.0 / 5.0));
        values.add(dataFactory.pairOf(1, 3.0 / 5.0));
        values.add(dataFactory.pairOf(0, 0.0 / 5.0));
        values.add(dataFactory.pairOf(1, 1.0 / 5.0));
        final List<PairOfDoubles> baseline = new ArrayList<>();
        baseline.add(dataFactory.pairOf(0, 2.0 / 5.0));
        baseline.add(dataFactory.pairOf(0, 2.0 / 5.0));
        baseline.add(dataFactory.pairOf(1, 5.0 / 5.0));
        baseline.add(dataFactory.pairOf(1, 3.0 / 5.0));
        baseline.add(dataFactory.pairOf(0, 4.0 / 5.0));
        baseline.add(dataFactory.pairOf(1, 1.0 / 5.0));
        final Metadata main = MetadataFactory.getMetadata(values.size(), MetadataFactory.getDimension(), "Main");
        final Metadata base = MetadataFactory.getMetadata(values.size(), MetadataFactory.getDimension(), "Baseline");
        return MetricInputFactory.ofDiscreteProbabilityPairs(values, baseline, main, base);
    }
}
