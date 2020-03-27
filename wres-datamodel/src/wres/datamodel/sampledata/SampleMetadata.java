package wres.datamodel.sampledata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.generated.ProjectConfig;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindow;

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
     * The optional time scale information, may be null.
     */

    private final TimeScale timeScale;

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
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeScale}.
     * 
     * @param input the source metadata
     * @param timeScale the new time scale
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( final SampleMetadata input, final TimeScale timeScale )
    {
        return new SampleMetadataBuilder().setFromExistingInstance( input ).setTimeScale( timeScale ).build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindow} and 
     * {@link TimeScale}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param timeScale the new time scale
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( final SampleMetadata input,
                                     final TimeWindow timeWindow,
                                     final TimeScale timeScale )
    {
        return new SampleMetadataBuilder().setFromExistingInstance( input )
                                          .setTimeWindow( timeWindow )
                                          .setTimeScale( timeScale )
                                          .build();
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

    public static SampleMetadata of( final SampleMetadata input,
                                     final TimeWindow timeWindow,
                                     final OneOrTwoThresholds thresholds )
    {
        return new SampleMetadataBuilder().setFromExistingInstance( input )
                                          .setThresholds( thresholds )
                                          .setTimeWindow( timeWindow )
                                          .build();
    }

    /**
     * Finds the union of the input, based on the {@link TimeWindow}. All components of the input must be equal, 
     * except the {@link SampleMetadata#getTimeWindow()} and {@link SampleMetadata#getThresholds()}, otherwise an 
     * exception is thrown. See also {@link TimeWindow#unionOf(List)}. No threshold information is represented in the 
     * union.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws IllegalArgumentException if the input is empty
     * @throws NullPointerException if the input is null
     * @throws SampleMetadataException if the input contains metadata whose differences extend beyond the time windows and
     *            thresholds
     */

    public static SampleMetadata unionOf( List<SampleMetadata> input )
    {
        String nullString = "Cannot find the union of null metadata.";

        Objects.requireNonNull( input, nullString );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot find the union of empty input." );
        }
        List<TimeWindow> unionWindow = new ArrayList<>();

        // Test entry
        SampleMetadata test = input.get( 0 );

        // Validate for equivalence with the first entry and add window to list
        for ( SampleMetadata next : input )
        {
            Objects.requireNonNull( next, nullString );

            if ( !next.equalsWithoutTimeWindowOrThresholds( test ) )
            {
                throw new SampleMetadataException( "Only the time window and thresholds can differ when finding the union of "
                                                   + "metadata." );
            }
            if ( next.hasTimeWindow() )
            {
                unionWindow.add( next.getTimeWindow() );
            }
        }

        // Remove any threshold information from the result
        test = of( test, (OneOrTwoThresholds) null );

        if ( !unionWindow.isEmpty() )
        {
            test = of( test, TimeWindow.unionOf( unionWindow ) );
        }
        return test;
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
        boolean returnMe = this.hasTimeWindow() == p.hasTimeWindow()
                           && this.hasThresholds() == p.hasThresholds()
                           && this.equalsWithoutTimeWindowOrThresholds( p );

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
                             this.hasTimeScale(),
                             this.getMeasurementUnit(),
                             this.getIdentifier(),
                             this.getTimeWindow(),
                             this.getThresholds(),
                             this.getProjectConfig(),
                             this.getTimeScale() );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "datasetIdentifier",
                                                                                     this.getIdentifier() )
                                                                            .append( "timeWindow",
                                                                                     this.getTimeWindow() )
                                                                            .append( "thresholds",
                                                                                     this.getThresholds() )
                                                                            .append( "timeScale",
                                                                                     this.getTimeScale() )
                                                                            .append( "measurementUnit",
                                                                                     this.getMeasurementUnit() )
                                                                            .build();
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
     * Returns <code>true</code> if {@link #getTimeScale()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getTimeScale()} returns non-null, false otherwise.
     */
    public boolean hasTimeScale()
    {
        return Objects.nonNull( this.getTimeScale() );
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
                           && this.hasProjectConfig() == input.hasProjectConfig()
                           && this.hasTimeScale() == input.hasTimeScale();

        // The following tests apply where both the existing and input attributes are non-null,
        // as equivalent null status is tested above
        if ( this.hasIdentifier() )
        {
            returnMe = returnMe && this.getIdentifier().equals( input.getIdentifier() );
        }

        if ( this.hasProjectConfig() )
        {
            returnMe = returnMe && this.getProjectConfig().equals( input.getProjectConfig() );
        }

        if ( this.hasTimeScale() )
        {
            returnMe = returnMe && this.getTimeScale().equals( input.getTimeScale() );
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
     * @return a time window or null
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
     * Returns a {@link TimeScale} associated with the metadata or null.
     * 
     * @return the time scale or null
     */

    public TimeScale getTimeScale()
    {
        return this.timeScale;
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
         * The optional time scale information.
         */

        private TimeScale timeScale;

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
         * Sets the time scale information.
         * 
         * @param timeScale the time scale
         * @return the builder
         */

        public SampleMetadataBuilder setTimeScale( TimeScale timeScale )
        {
            this.timeScale = timeScale;
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
            this.timeScale = sampleMetadata.timeScale;

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
        this.timeScale = builder.timeScale;

        Objects.requireNonNull( this.unit,
                                "Specify a non-null measurement unit from which to build "
                                           + "the metadata." );

    }

}
