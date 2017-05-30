package wres.io.grouping;

import java.util.List;

import wres.io.config.specification.MetricSpecification;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * @author Christopher Tubbs
 *
 */
public class Contingency
{

    /**
     * Creates the contingency "matrix" by unpacking the pairs
     * 
     * TODO: Pass in a specification structure dictating what constitutes a yes or no
     * for the forecasts and observations
     */
    public Contingency(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            boolean leftPositive = pair.getItemOne() > 0;
            boolean rightPositive = false;
            for (double rightValue : pair.getItemTwo())
            {
                rightPositive = rightValue > 0;

                if (leftPositive)
                {
                    this.totalPositiveObservations++;
                }
                else {
                    this.totalNegativeObservations++;
                }
                
                if (rightPositive)
                {
                    this.totalPositiveForecasts++;
                }
                else
                {
                    this.totalNegativeForecasts++;
                }
                
                if (leftPositive && rightPositive)
                {
                    this.numberOfHits++;
                }
                else if (leftPositive && !rightPositive)
                {
                    this.numberOfMisses++; 
                }
                else if (!leftPositive && rightPositive)
                {
                    this.numberOfFalseAlarms++;
                }
                else if (!leftPositive && !rightPositive)
                {
                    this.numberOfCorrectNegatives++;
                }
            }
        }
    }
    
    public Contingency(List<PairOfDoubleAndVectorOfDoubles> pairs, MetricSpecification specification)
    {
        // Absolute is easy; for both sides, return 1 if above, 0 if below, but what
        // of the left hand side for the rest? is it just always marked as a hit?
    }
    
    public double getNumberOfHits()
    {
        return this.numberOfHits;
    }
    
    public double getNumberOfMisses()
    {
        return this.numberOfMisses;
    }
    
    public double getNumberOfFalseAlarms()
    {
        return this.numberOfFalseAlarms;
    }
    
    public double getNumberOfCorrectNegatives()
    {
        return this.numberOfCorrectNegatives;
    }
    
    public double getNumberOfPositiveForecasts()
    {
        return this.totalPositiveForecasts;
    }
    
    public double getTotalNegativeForecasts()
    {
        return this.totalNegativeForecasts;
    }
    
    public double getTotalPositiveObservations()
    {
        return this.totalPositiveObservations;
    }
    
    public double getTotalNegativeObservations()
    {
        return this.totalNegativeObservations;
    }
    
    public double getTotalMeasurements()
    {
        return this.numberOfMisses + 
               this.numberOfHits + 
               this.numberOfCorrectNegatives + 
               this.numberOfFalseAlarms;
    }
    
    public double getBias()
    {
        double bias = 0.0;
        
        if (this.getTotalMeasurements() != 0)
        {
            bias = (this.numberOfHits + this.numberOfFalseAlarms) / (this.numberOfHits + this.numberOfFalseAlarms);
        }
        
        return bias;
    }
    
    public double getAccuracy()
    {
        double accuracy = 0.0;
        
        if (this.getTotalMeasurements() != 0)
        {
            accuracy = (this.numberOfHits + this.numberOfCorrectNegatives) / this.getTotalMeasurements();
        }
        
        return accuracy;
    }
    
    public double getProbabilityOfDetection()
    {
        double POD = 0.0;
        
        if (this.getTotalMeasurements() != 0)
        {
            POD = this.numberOfHits / (this.numberOfHits + this.numberOfMisses);
        }
        
        return POD;
    }
    
    public double getFalseAlarmRatio()
    {
        double FAR = 0.0;
        
        if (this.getTotalMeasurements() != 0)
        {
            FAR = this.numberOfFalseAlarms / (this.numberOfHits + this.numberOfFalseAlarms); 
        }
        
        return FAR;
    }
    
    public double getProbabilityOfFalseDetection()
    {
        double POFD = 0.0;
        
        if (this.getTotalMeasurements() != 0)
        {
            POFD = this.numberOfFalseAlarms / (this.numberOfCorrectNegatives + this.numberOfFalseAlarms);
        }
        
        return POFD;
    }
    
    public double getSuccessRatio()
    {
        double SR = 0.0;
        
        if (this.getTotalMeasurements() != 0)
        {
            SR = this.numberOfHits / (this.numberOfHits + this.numberOfFalseAlarms);
        }
        return SR;
    }
    
    public double getEquitableThreatScore()
    {
        if (this.getTotalMeasurements() == 0)
        {
            return 0.0;
        }
        
        double randomHits = ((this.numberOfHits + this.numberOfMisses)* (this.numberOfHits + this.numberOfFalseAlarms));
        randomHits /= this.getTotalMeasurements();
        
        double ETS = this.numberOfHits - randomHits;
        ETS /= this.numberOfHits +
               this.numberOfMisses +
               this.numberOfFalseAlarms -
               randomHits;
        
        return ETS;
    }
    
    public double getHKDiscriminant()
    {
        return this.getProbabilityOfDetection() - this.getProbabilityOfFalseDetection();
    }
    
    public double getHeidkeSkillScore()
    {
        if (this.getTotalMeasurements() == 0)
        {
            return 0.0;
        }
        
        double expectedCorrect = this.totalPositiveForecasts * this.totalPositiveObservations;
        expectedCorrect += this.totalNegativeForecasts * this.totalNegativeObservations;
        double HSS = this.numberOfHits + this.numberOfCorrectNegatives;
        HSS -= expectedCorrect;
        HSS /= (this.getTotalMeasurements() - expectedCorrect);
        return HSS;
    }
    
    public double getOddsRatio()
    {
        double ratio = this.getProbabilityOfDetection() / (1 - this.getProbabilityOfDetection());
        ratio /= this.getProbabilityOfFalseDetection() / (1 - this.getProbabilityOfFalseDetection());
        
        return ratio;
    }
    
    public double getOddsRationSkillScore()
    {
        double ORSS = (this.numberOfHits * this.numberOfCorrectNegatives) + (this.numberOfMisses * this.numberOfFalseAlarms);
        ORSS /= (this.numberOfHits * this.numberOfCorrectNegatives) + (this.numberOfMisses * this.numberOfFalseAlarms);
        
        return ORSS;
    }

    private double numberOfHits;
    private double numberOfMisses;
    private double numberOfFalseAlarms;
    private double numberOfCorrectNegatives;
    private double totalPositiveForecasts;
    private double totalNegativeForecasts;
    private double totalPositiveObservations;
    private double totalNegativeObservations;
}
