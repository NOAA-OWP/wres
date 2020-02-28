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
         * The time at which a model begins forward integration into a forecasting horizon, a.k.a. a forecast 
         * initialization time.
         */
        
        T0,
        
        /**
         * The start time of an analysis and assimilation period. The model begins forward integration at this time 
         * and continues until the forecast initialization time or {@link #T0}.
         */
        
        ANALYSIS_START_TIME,
        
        /**
         * The time at which a time-series was published or "issued".
         */
        
        ISSUED_TIME,

        /**
         * The time at which the latest observed value was included/assimilated.
         */
        
        LATEST_OBSERVATION;
}
