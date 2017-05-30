package wres.util;

import java.util.ArrayList;
import java.util.List;

import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * @author Christopher Tubbs
 *
 */
public final class DataModel
{
    
    public static double getPairedDoubleMean(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        double mean = 0.0;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            mean += pair.getItemOne();
        }
        
        mean /= pairs.size();
        return mean;
    }
    
    public static double getPairedDoubleVectorMean(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        double mean = 0.0;
        int totalVectorValues = 0;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            for (int pairIndex = 0; pairIndex < pair.getItemTwo().length; ++pairIndex)
            {
                mean += pair.getItemTwo()[pairIndex];
                totalVectorValues++;
            }
        }
        
        mean /= totalVectorValues; 
        return mean;
    }
    
    public static double getPairedDoubleStandardDeviation(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        if (pairs.size() == 1)
        {
            return 0.0;
        }
        
        final double mean = getPairedDoubleMean(pairs);
        int pairCount = 0;
        double STD = 0.0;       
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            STD += Math.pow(pair.getItemOne() - mean, 2) * pair.getItemTwo().length;
            pairCount += pair.getItemTwo().length;
        }
        
        STD /= (pairCount - 1);
        
        return Math.sqrt(STD);
    }
    
    public static double getPairedDoubleVectorStandardDeviation(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        if (pairs.size() == 1)
        {
            return 0.0;
        }
        
        final double mean = getPairedDoubleVectorMean(pairs);
        double STD = 0.0;   
        int pairedValueCount = 0;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            for (int rightIndex = 0; rightIndex < pair.getItemTwo().length; ++rightIndex)
            {
                STD += Math.pow(pair.getItemTwo()[rightIndex] - mean, 2);
                pairedValueCount++;
            }
        }
        
        STD /= (pairedValueCount - 1);
        
        return Math.sqrt(STD);
    }
    
    public static double getCovariance(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        if (pairs.size() == 1)
        {
            return 0.0;
        }
        
        double pairedDoubleMean = getPairedDoubleMean(pairs);
        double pairedDoubleVectorMean = getPairedDoubleVectorMean(pairs);
        double pairCount = 0.0;
        
        double covSum = 0.0;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            for (int rightIndex = 0; rightIndex < pair.getItemTwo().length; ++rightIndex)
            {
                covSum += (pair.getItemOne() - pairedDoubleMean) * (pair.getItemTwo()[rightIndex] - pairedDoubleVectorMean);
                pairCount++;
            }
        }
        
        return covSum / (pairCount - 1);
    }
    
    public static Double getPairedDoubleVectorMax(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        Double maximum = null;
        
        List<Double> maximumValues = new ArrayList<>(pairs.size());
        
        pairs.parallelStream().forEach((PairOfDoubleAndVectorOfDoubles pair) -> {
            Double max = null;
            for (int memberIndex = 0; memberIndex < pair.getItemTwo().length; ++memberIndex)
            {
                if (max == null || pair.getItemTwo()[memberIndex] > max)
                {
                    max = pair.getItemTwo()[memberIndex];
                }
            }
            if (max != null)
            {
                maximumValues.add(max);
            }
        });
        
        for (Double value : maximumValues)
        {
            if (maximum == null || value > maximum)
            {
                maximum = value;
            }
        }
        
        return maximum;
    }
    
    public static Double getPairedDoubleVectorMin(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        Double minimum = null;
        
        List<Double> minimumValues = new ArrayList<>(pairs.size());
        
        pairs.parallelStream().forEach((PairOfDoubleAndVectorOfDoubles pair) -> {
            Double max = null;
            for (int memberIndex = 0; memberIndex < pair.getItemTwo().length; ++memberIndex)
            {
                if (max == null || pair.getItemTwo()[memberIndex] < max)
                {
                    max = pair.getItemTwo()[memberIndex];
                }
            }
            if (max != null)
            {
                minimumValues.add(max);
            }
        });
        
        for (Double value : minimumValues)
        {
            if (minimum == null || value < minimum)
            {
                minimum = value;
            }
        }
        
        return minimum;
    }
    
    public static Double getMinimumPairedDouble(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        Double minimum = null;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            if (minimum == null || pair.getItemOne() < minimum)
            {
                minimum = pair.getItemOne();
            }
        }
        
        return minimum;
    }
    
    public static Double getMaximumPairedDouble(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        Double maximum = null;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            if (maximum == null || pair.getItemOne() > maximum)
            {
                maximum = pair.getItemOne();
            }
        }
        
        return maximum;
    }
}
