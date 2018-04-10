package wres.datamodel;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import wres.datamodel.ThresholdConstants.ThresholdGroup;

public class SafeThresholdsByType implements ThresholdsByType
{

    /**
     * The thresholds by type.
     */

    private final Map<ThresholdGroup, Set<Threshold>> thresholdsByType;

    @Override
    public Set<Threshold> getThresholdsByType( ThresholdGroup type )
    {
        if ( this.contains( type ) )
        {
            return Collections.unmodifiableSet( thresholdsByType.get( type ) );
        }
        return Collections.emptySet();
    }

    @Override
    public boolean contains( ThresholdGroup type )
    {
        Objects.requireNonNull( type, "Specify a non-null threshold type." );

        return thresholdsByType.containsKey( type );
    }

    @Override
    public Set<ThresholdGroup> getAllThresholdTypes()
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
        for ( ThresholdGroup next : thresholds.getAllThresholdTypes() )
        {
            builder.addThresholds( thresholds.getThresholdsByType( next ), next );
        }
        
        // Add this
        for ( ThresholdGroup next : this.getAllThresholdTypes() )
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

        private final Map<ThresholdGroup, Set<Threshold>> thresholdsByType = new EnumMap<>( ThresholdGroup.class );

        @Override
        public ThresholdsByTypeBuilder addThresholds( Set<Threshold> thresholds,
                                                      ThresholdGroup thresholdType )
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
