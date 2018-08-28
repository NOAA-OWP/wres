package wres.datamodel.metadata;

import java.util.Comparator;
import java.util.Objects;

import wres.config.generated.ProjectConfig;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * An immutable store of metadata associated with {@link SampleData}. Includes a {@link SampleMetadataBuilder} for 
 * incremental construction.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SampleMetadata implements Comparable<SampleMetadata>
{

    /**
     * The measurement unit associated with the data.
     */

    private final MeasurementUnit unit;

    /**
     * An optional dataset identifier, may be null.
     */

    private final DatasetIdentifier identifier;

    /**
     * An optional time window associated with the data, may be null.
     */

    private final TimeWindow timeWindow;

    /**
     * An optional set of thresholds associated with the data, may be null.
     */

    private final OneOrTwoThresholds thresholds;

    /**
     * The optional {@link ProjectConfig} associated with the metadata, may be null.
     */

    private final ProjectConfig projectConfig;

    /**
     * Build a {@link SampleMetadata} object with a default {@link MeasurementUnit}.
     * 
     * @return a {@link SampleMetadata} object
     */

    public static SampleMetadata of()
    {
        return new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() ).build();
    }

    /**
     * Build a {@link SampleMetadata} object with a sample size and a prescribed {@link MeasurementUnit}.
     * 
     * @param unit the required measurement unit
     * @return a {@link SampleMetadata} object
     */
    
    public static SampleMetadata of( final MeasurementUnit unit )
    {
        return new SampleMetadataBuilder().setMeasurementUnit( unit ).build();
    }

    /**
     * Build a {@link SampleMetadata} object with a prescribed {@link MeasurementUnit} and an optional {@link DatasetIdentifier}.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link SampleMetadata} object
     */

    public static SampleMetadata of( final MeasurementUnit unit, final DatasetIdentifier identifier )
    {
        return new SampleMetadataBuilder().setMeasurementUnit( unit ).setIdentifier( identifier ).build();
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @param thresholds an optional set of thresholds
     * @throws NullPointerException if the dimension is null
     * @return a metadata instance
     */

    public static SampleMetadata of( final MeasurementUnit unit,
                                     final DatasetIdentifier identifier,
                                     final TimeWindow timeWindow,
                                     final OneOrTwoThresholds thresholds )
    {
        return new SampleMetadataBuilder().setMeasurementUnit( unit )
                                          .setIdentifier( identifier )
                                          .setTimeWindow( timeWindow )
                                          .setThresholds( thresholds )
                                          .build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param thresholds the thresholds
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( final SampleMetadata input, final OneOrTwoThresholds thresholds )
    {
        return new SampleMetadataBuilder().setFromExistingInstance( input ).setThresholds( thresholds ).build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindow}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( final SampleMetadata input, final TimeWindow timeWindow )
    {
        return new SampleMetadataBuilder().setFromExistingInstance( input ).setTimeWindow( timeWindow ).build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindow} and 
     * {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param thresholds the thresholds
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata
            of( final SampleMetadata input, final TimeWindow timeWindow, final OneOrTwoThresholds thresholds )
    {
        return new SampleMetadataBuilder().setFromExistingInstance( input )
                                          .setThresholds( thresholds )
                                          .setTimeWindow( timeWindow ).build();
    }

    @Override
    public int compareTo( SampleMetadata input )
    {
        Objects.requireNonNull( input, "Specify non-null metadata for comparison." );

        // Check measurement units, which are always available
        int returnMe = this.getMeasurementUnit().compareTo( input.getMeasurementUnit() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check identifier via the string representation
        returnMe = Objects.compare( this.getIdentifier() + "", input.getIdentifier() + "", Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check the time window
        Comparator<TimeWindow> compareWindows = Comparator.nullsFirst( Comparator.naturalOrder() );
        returnMe = Objects.compare( this.getTimeWindow(), input.getTimeWindow(), compareWindows );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check the thresholds
        Comparator<OneOrTwoThresholds> compareThresholds = Comparator.nullsFirst( Comparator.naturalOrder() );
        returnMe = Objects.compare( this.getThresholds(), input.getThresholds(), compareThresholds );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check the project configuration
        Comparator<ProjectConfig> compareProjects = Comparator.nullsFirst( Comparator.naturalOrder() );
        return Objects.compare( this.getProjectConfig(), input.getProjectConfig(), compareProjects );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SampleMetadata ) )
        {
            return false;
        }
        final SampleMetadata p = (SampleMetadata) o;
        boolean returnMe = this.equalsWithoutTimeWindowOrThresholds( p ) && this.hasTimeWindow() == p.hasTimeWindow()
                           && this.hasThresholds() == p.hasThresholds();

        if ( returnMe && hasTimeWindow() )
        {
            returnMe = this.getTimeWindow().equals( p.getTimeWindow() );
        }

        if ( returnMe && hasThresholds() )
        {
            returnMe = this.getThresholds().equals( p.getThresholds() );
        }

        return returnMe;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.hasIdentifier(),
                             this.hasTimeWindow(),
                             this.hasThresholds(),
                             this.hasProjectConfig(),
                             this.getMeasurementUnit(),
                             this.getIdentifier(),
                             this.getTimeWindow(),
                             this.getThresholds(),
                             this.getProjectConfig() );
    }

    @Override
    public String toString()
    {
        final StringBuilder b = new StringBuilder();
        if ( hasIdentifier() )
        {
            String appendMe = this.identifier.toString();
            appendMe = appendMe.replaceAll( "\\]", "," );
            appendMe = appendMe.replaceAll( "\\[", "(" );
            b.append( appendMe );
        }
        else
        {
            b.append( "(" );
        }
        if ( this.hasTimeWindow() )
        {
            b.append( this.getTimeWindow() ).append( "," );
        }
        if ( this.hasThresholds() )
        {
            b.append( this.getThresholds() ).append( "," );
        }

        b.append( this.getMeasurementUnit() ).append( ")" );

        return b.toString();
    }


    /**
     * Returns <code>true</code> if {@link #getIdentifier()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getIdentifier()} returns non-null, false otherwise.
     */
    public boolean hasIdentifier()
    {
        return Objects.nonNull( this.getIdentifier() );
    }

    /**
     * Returns <code>true</code> if {@link #getTimeWindow()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getTimeWindow()} returns non-null, false otherwise.
     */
    public boolean hasTimeWindow()
    {
        return Objects.nonNull( this.getTimeWindow() );
    }

    /**
     * Returns <code>true</code> if {@link #getThresholds()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getThresholds()} returns non-null, false otherwise.
     */
    public boolean hasThresholds()
    {
        return Objects.nonNull( this.getThresholds() );
    }

    /**
     * Returns <code>true</code> if {@link #getProjectConfig()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getProjectConfig()} returns non-null, false otherwise.
     */
    public boolean hasProjectConfig()
    {
        return Objects.nonNull( this.getProjectConfig() );
    }

    /**
     * Returns <code>true</code> if the input is equal to the current {@link SampleMetadata} without considering the 
     * {@link #getTimeWindow()} or {@link #getThresholds()}.
     * 
     * @param input the input metadata
     * @return true if the input is equal to the current metadata, without considering the time window or thresholds
     */
    public boolean equalsWithoutTimeWindowOrThresholds( final SampleMetadata input )
    {
        if ( Objects.isNull( input ) )
        {
            return false;
        }
        boolean returnMe =
                input.getMeasurementUnit().equals( this.getMeasurementUnit() )
                           && this.hasIdentifier() == input.hasIdentifier()
                           && this.hasProjectConfig() == input.hasProjectConfig();

        if ( hasIdentifier() )
        {
            returnMe = returnMe && this.getIdentifier().equals( input.getIdentifier() );
        }

        if ( hasProjectConfig() )
        {
            returnMe = returnMe && this.getProjectConfig().equals( input.getProjectConfig() );
        }


        return returnMe;
    }

    /**
     * Returns the measurement unit associated with the metric.
     * 
     * @return the measurement unit
     */

    public MeasurementUnit getMeasurementUnit()
    {
        return this.unit;
    }

    /**
     * Returns an optional dataset identifier or null.
     * 
     * @return an identifier or null
     */

    public DatasetIdentifier getIdentifier()
    {
        return this.identifier;
    }

    /**
     * Returns a {@link TimeWindow} associated with the metadata or null.
     * 
     * @return a lead time or null
     */

    public TimeWindow getTimeWindow()
    {
        return this.timeWindow;
    }

    /**
     * Returns a {@link OneOrTwoThresholds} associated with the metadata or null.
     * 
     * @return a set of thresholds or null
     */

    public OneOrTwoThresholds getThresholds()
    {
        return this.thresholds;
    }

    /**
     * Returns a {@link ProjectConfig} associated with the metadata or null.
     * 
     * @return the project declaration or null
     */

    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    /**
     * Builder.
     */

    public static class SampleMetadataBuilder
    {

        /**
         * Error message for null input.
         */

        private static final String NULL_INPUT_ERROR = "Specify a non-null source from which to build the metadata.";
        
        /**
         * The measurement unit associated with the data.
         */

        private MeasurementUnit unit;

        /**
         * An optional dataset identifier, may be null.
         */

        private DatasetIdentifier identifier;

        /**
         * An optional time window associated with the data, may be null.
         */

        private TimeWindow timeWindow;

        /**
         * An optional set of thresholds associated with the data, may be null.
         */

        private OneOrTwoThresholds thresholds;

        /**
         * The optional {@link ProjectConfig} associated with the metadata, may be null.
         */

        private ProjectConfig projectConfig;

        /**
         * Sets the measurement unit.
         * 
         * @param unit the measurement unit
         * @return the builder
         */

        public SampleMetadataBuilder setMeasurementUnit( MeasurementUnit unit )
        {
            this.unit = unit;
            return this;
        }

        /**
         * Sets the identifier.
         * 
         * @param identifier the identifier
         * @return the builder
         */

        public SampleMetadataBuilder setIdentifier( DatasetIdentifier identifier )
        {
            this.identifier = identifier;
            return this;
        }

        /**
         * Sets the time window.
         * 
         * @param timeWindow the time window
         * @return the builder
         */

        public SampleMetadataBuilder setTimeWindow( TimeWindow timeWindow )
        {
            this.timeWindow = timeWindow;
            return this;
        }

        /**
         * Sets the thresholds.
         * 
         * @param thresholds the thresholds
         * @return the builder
         */

        public SampleMetadataBuilder setThresholds( OneOrTwoThresholds thresholds )
        {
            this.thresholds = thresholds;
            return this;
        }

        /**
         * Sets the project declaration.
         * 
         * @param projectConfig the project declaration
         * @return the builder
         */

        public SampleMetadataBuilder setProjectConfig( ProjectConfig projectConfig )
        {
            this.projectConfig = projectConfig;
            return this;
        }
        
        /**
         * Sets the contents from an existing metadata instance.
         * 
         * @param sampleMetadata the source metadata
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public SampleMetadataBuilder setFromExistingInstance( SampleMetadata sampleMetadata )
        {
            Objects.requireNonNull( sampleMetadata, NULL_INPUT_ERROR );
            
            this.unit = sampleMetadata.unit;
            this.identifier = sampleMetadata.identifier;
            this.timeWindow = sampleMetadata.timeWindow;
            this.thresholds = sampleMetadata.thresholds;
            this.projectConfig = sampleMetadata.projectConfig;
            
            return this;
        }
        
        /**
         * Build the metadata.
         * 
         * @return the metadata instance
         */
        
        public SampleMetadata build()
        {
            return new SampleMetadata( this );
        }
        
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws NullPointerException if the measurement unit has not been set
     */

    private SampleMetadata( SampleMetadataBuilder builder )
    {
        // Set then validate
        this.unit = builder.unit;
        this.identifier = builder.identifier;
        this.timeWindow = builder.timeWindow;
        this.thresholds = builder.thresholds;
        this.projectConfig = builder.projectConfig;

        Objects.requireNonNull( this.unit,
                                "Specify a non-null measurement unit from which to build "
                                           + "the metadata." );

    }

}
