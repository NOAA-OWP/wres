package wres.io.grouping;

import java.util.Objects;

/**
 * @author Christopher Tubbs
 *
 */
public final class LeadResult implements Comparable<LeadResult>
{
    /**
     * Creates the immutable pair of two values
     */
    public LeadResult(int lead, double result) {
        this.lead = lead;
        this.result = result;
    }

    public int getLead()
    {
        return this.lead;
    }

    public double getResult() {
        return this.result;
    }
    
    public LeadResult copy() 
    {
        return new LeadResult(this.lead, this.result);
    }
    
    @Override
    public String toString()
    {
        return "( lead = " + String.valueOf(this.lead) + ", result: " + String.valueOf(this.result) + ")";
    }
    
    @Override
    public int hashCode()
    {
        return Objects.hash(this.lead, this.result);
    }

    @Override
    public int compareTo(final LeadResult other)
    {
        int equality;
        
        if (this.lead < other.lead)
        {
            equality = -1;
        }
        else if (this.lead > other.lead)
        {
            equality = 1;
        }
        else
        {
            equality = 0;
        }
        
        if (equality == 0)
        {
            if (this.result < other.result)
            {
                equality = -1;
            }
            else if (this.result > other.result)
            {
                equality = 1;
            }
            else
            {
                equality = 0;
            }
        }

        return equality;
    }
    
    int lead;
    double result;
}
