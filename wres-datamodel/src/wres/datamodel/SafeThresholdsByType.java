package wres.datamodel;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import wres.datamodel.ThresholdConstants.ThresholdType;

public class SafeThresholdsByType implements ThresholdsByType
{

    /**
     * The thresholds by type.
     */

    private final Map<ThresholdType, Set<Threshold>> thresholdsByType;

    @Override
    public Set<Threshold> getThresholdsByType( ThresholdType type )
    {
        if ( this.contains( type ) )
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
    public Set<ThresholdType> getAllThresholdTypes()
    {
        return Collections.unmodifiableSet( thresholdsByType.keySet() );
    }

    @Override
    public ThresholdsByType union( ThresholdsByType thresholds )
    {
        Objects.requireNonNull( thresholdsByType, "Specify non-null thresholds from which to "
                                                  + "obtain the union." );

        SafeThresholdsByTypeBuilder builder = new SafeThresholdsByTypeBuilder();
        
        // Add inputs
        for ( ThresholdType next : thresholds.getAllThresholdTypes() )
        {
            builder.addThresholds( thresholds.getThresholdsByType( next ), next );
        }
        
        // Add this
        for ( ThresholdType next : this.getAllThresholdTypes() )
        {
            builder.addThresholds( this.getThresholdsByType( next ), next );
        }
        
        return builder.build();
    }
    
    /**
     * Builder.
     */

    static class SafeThresholdsByTypeBuilder implements ThresholdsByTypeBuilder
    {

        /**
         * The thresholds by type.
         */

        private final Map<ThresholdType, Set<Threshold>> thresholdsByType = new EnumMap<>( ThresholdType.class );

        @Override
        public ThresholdsByTypeBuilder addThresholds( Set<Threshold> thresholds,
                                                      ThresholdType thresholdType )
        {
            Objects.requireNonNull( thresholds, "Specify non-null thresholds." );

            Objects.requireNonNull( thresholds, "Specify a non-null threshold type." );

            // Set the data
            if ( thresholdsByType.containsKey( thresholdType ) )
            {
                thresholdsByType.get( thresholdType ).addAll( thresholds );
            }
            else
            {
                thresholdsByType.put( thresholdType, new HashSet<>(thresholds) );
            }

            return this;
        }

        @Override
        public ThresholdsByType build()
        {
            return new SafeThresholdsByType( this );
        }

    }

    /**
     * Build a {@link ThresholdsByType}
     * 
     * @param builder the builder
     */

    private SafeThresholdsByType( SafeThresholdsByTypeBuilder builder )
    {
        this.thresholdsByType = Collections.unmodifiableMap( new HashMap<>( builder.thresholdsByType ) );
    }
    
}
