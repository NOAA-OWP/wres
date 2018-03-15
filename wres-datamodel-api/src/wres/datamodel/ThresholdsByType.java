package wres.datamodel;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A container of {@link Threshold} by {@link ThresholdType}.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface ThresholdsByType
{
    
    /**
     * An enumeration of threshold types.
     */
    
    public enum ThresholdType
    {
        
        /**
         * Probability threshold.
         */
        
        PROBABILITY,
        
        /**
         * Value threshold.
         */
        
        VALUE,
        
        /**
         * Quantile threshold.
         */
        
        QUANTILE,
        
        /**
         * Probability classifier threshold.
         */
        
        PROBABILITY_CLASSIFIER;        
        
    }
    
    /**
     * Returns a set of {@link Threshold} for a specified type or null.
     * 
     * @param type the type of threshold
     * @return the thresholds for a specified type or null
     */
    
    Set<Threshold> getThresholdsByType( ThresholdType type );
    
    /**
     * Returns <code>true</code> if the store contains thresholds for the specified type, otherwise <code>false</code>.
     * 
     * @param type the type of threshold 
     * @return true if the store contains the type, otherwise false 
     * @throws NullPointerException if the input is null
     */
    
    boolean contains( ThresholdType type );
    
    /**
     * Returns the set of {@link ThresholdType} in the store.
     * 
     * @return the threshold types stored
     */
    
    Set<ThresholdType> getStoredTypes();
    
    /**
     * Combines the input with the contents of the current container, return a new container that reflects the union
     * of the two.
     * 
     * @param thresholds the thresholds
     * @return the union of the input and the current thresholds
     * @throws NullPointerException if the input is null
     */
    
    ThresholdsByType union( ThresholdsByType thresholds );
    
    /**
     * Returns a default immutable implementation of a {@link ThresholdsByType} for one threshold type.
     * 
     * @param type the threshold type
     * @param thresholds the set of thresholds
     * @return a default implementation of a threshold container
     * @throws NullPointerException if either input is null
     */
    
    static ThresholdsByType of( ThresholdType type, Set<Threshold> thresholds )
    {
        Objects.requireNonNull( type, "Specify a non-null threshold type." );
        
        Objects.requireNonNull( thresholds, "Specify a non-null set of thresholds." );
        
        Map<ThresholdType, Set<Threshold>> thresholdsByType = new EnumMap<>( ThresholdType.class );
        
        thresholdsByType.put( type, thresholds );
        
        return of( thresholdsByType );
    }
    
    /**
     * Returns a default immutable implementation of a {@link ThresholdsByType} for multiple threshold types.
     * 
     * @param thresholdsByType the thresholds by type
     * @return a default implementation of a threshold container
     */
    
    static ThresholdsByType of( Map<ThresholdType, Set<Threshold>> thresholdsByType )
    {
        /**
         * Default implementation of a {@link ThresholdsByType}.
         */
        
        class DefaultThresholdsByType implements ThresholdsByType
        {  
            
            /**
             * The thresholds by type.
             */
            
            private final Map<ThresholdType, Set<Threshold>> thresholdsByType;

            /**
             * Build a {@link ThresholdsByType}
             * 
             * @param time the time
             * @param value the value
             * @throws NullPointerException if the input is null
             * @throws IllegalArgumentException if the input is empty
             */

            private DefaultThresholdsByType( Map<ThresholdType, Set<Threshold>> thresholdsByType )
            {
                Objects.requireNonNull( thresholdsByType, "Specify a non-null map of thresholds by type." );
                
                if( thresholdsByType.isEmpty() )
                {
                    throw new IllegalArgumentException( "Specify a map of thresholds with at least one set of "
                            + "thresholds by type." );
                }
                
                this.thresholdsByType = Collections.unmodifiableMap( new HashMap<>( thresholdsByType ) );
            }

            @Override
            public Set<Threshold> getThresholdsByType( ThresholdType type )
            {
                if( this.contains( type ) )
                {
                    return Collections.unmodifiableSet( thresholdsByType.get( type ) );
                }
                return Collections.emptySet();
            }

            @Override
            public boolean contains( ThresholdType type )
            {
                Objects.requireNonNull( type, "Specify a non-null threshold type." );
                
                return thresholdsByType.containsKey( type );
            }

            @Override
            public Set<ThresholdType> getStoredTypes()
            {
                return Collections.unmodifiableSet( thresholdsByType.keySet() );
            }

            @Override
            public ThresholdsByType union( ThresholdsByType thresholds )
            {
                Objects.requireNonNull( thresholdsByType, "Specify non-null thresholds from which to "
                        + "obtain the union." );
                
                Map<ThresholdType,Set<Threshold>> union = new EnumMap<>( ThresholdType.class );
                union.putAll( this.thresholdsByType );
                
                for( ThresholdType next : thresholds.getStoredTypes() )
                {
                    union.put( next, thresholds.getThresholdsByType( next ) );
                }
                return new DefaultThresholdsByType( union );
            }
        }
        
        return new DefaultThresholdsByType( thresholdsByType );
    }    

}
