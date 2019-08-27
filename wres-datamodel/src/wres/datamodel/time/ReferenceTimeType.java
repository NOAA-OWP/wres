package wres.datamodel.time;

/**
 * Represents a type of reference time.
 * 
 * @author james.brown@hydrosolved.com
 */

public enum ReferenceTimeType
{

        /**
         * A default reference time type.
         */
        
        DEFAULT,
        
        /**
         * A model initialization time or T0.
         */
        
        T0,
        
        /**
         * The time at which a time-series was published or "issued".
         */
        
        ISSUED_TIME;
}
