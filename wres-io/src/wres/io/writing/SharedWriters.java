package wres.io.writing;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.DestinationType;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.io.writing.netcdf.NetcdfDoubleScoreWriter;

/**
 * Contains a set of shared writers. This is useful for managing the shared state of outputs that are written 
 * incrementally.  
 * 
 * @author james.brown@hydrosolved.com
 */
public class SharedWriters
{

    /**
     * Instance of a {@link NetcdfDoubleScoreWriter}
     */
    
    private final NetcdfDoubleScoreWriter netcdfDoublescoreWriter;
    
    /**
     * Set of types for which writers are available.
     */
    
    private final Set<Pair<DestinationType,MetricOutputGroup>> storedTypes;
    
    /**
     * Returns <code>true</code> if a writer is available for the specified types, otherwise <code>false</code>.
     * 
     * @param type the data type
     * @param format the output format
     * @return true if a writer is available for the input types, otherwise false
     * @throws NullPointerException if either input is null
     */

    public boolean contains( MetricOutputGroup type, DestinationType format  )
    {
        Objects.requireNonNull( type, "Specify a non-null type to test." );
        
        Objects.requireNonNull( format, "Specify a non-null format to test." );
        
        return storedTypes.contains( Pair.of( format, type ) );
    }
    
    /**
     * Returns a {@link NetcdfDoubleScoreWriter} or null.
     * 
     * @return a writer or null
     */
    
    public NetcdfDoubleScoreWriter getNetcdfDoubleScoreWriter()
    {
        return netcdfDoublescoreWriter;
    }
    
    /**
     * Use a builder to add writers.
     */
    
    public static class SharedWritersBuilder 
    {
        
        /**
         * Instance of a {@link NetcdfDoubleScoreWriter}
         */
        
        private NetcdfDoubleScoreWriter netcdfDoublescoreWriter;        
        
        /**
         * Sets a {@link NetcdfDoubleScoreWriter}.
         * 
         * @param netcdfDoublescoreWriter the writer
         * @return the builder
         */
        
        public SharedWritersBuilder setNetcdfDoublescoreWriter( NetcdfDoubleScoreWriter netcdfDoublescoreWriter )
        {
            this.netcdfDoublescoreWriter = netcdfDoublescoreWriter;
            return this;
        }
        
        /**
         * Return an instance of a {@link SharedWriter}.
         * 
         * @return a container for sharing writers
         */
        
        public SharedWriters build()
        {
            return new SharedWriters( this );
        }
    }
    
    
    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SharedWriters( SharedWritersBuilder builder )
    {
        // Set
        this.netcdfDoublescoreWriter = builder.netcdfDoublescoreWriter;
        
        // Register the stored types
        Set<Pair<DestinationType,MetricOutputGroup>> localTypes = new HashSet<>();
        
        if( Objects.nonNull( this.netcdfDoublescoreWriter ) )
        {
            localTypes.add( Pair.of( DestinationType.NETCDF, MetricOutputGroup.DOUBLE_SCORE ) );
        }
        
        this.storedTypes = Collections.unmodifiableSet( localTypes );
    }

}
