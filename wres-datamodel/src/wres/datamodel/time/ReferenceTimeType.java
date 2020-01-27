package wres.datamodel.time;

/**
 * Represents a type of reference time.
 * 
 * @author james.brown@hydrosolved.com
 */

public enum ReferenceTimeType
{

        /**
         * An unknown reference time type.
         */
        
        UNKNOWN,
        
        /**
         * A model initialization time or T0.
         */
        
        T0,
        
        /**
         * The time at which a time-series was published or "issued".
         */
        
        ISSUED_TIME,

        /**
         * The time at which the latest observed value was included/assimilated.
         */
        LATEST_OBSERVATION;
}
