package wres.io.config;

import java.time.Duration;

import wres.config.generated.Feature;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.TimeScale;
import wres.datamodel.metadata.TimeWindow;
import wres.io.project.Project;

/**
 * Container for metadata describing sample data for metrics
 * with the option for ordering in evaluation order
 * <p>
 *     If the sample number of the metadata is "5", it means that the sample
 *     that this metadata belongs to will be the 5th sample to be evaluated
 * </p>
 */
public class OrderedSampleMetadata
{
    /**
     * Metadata describing the primary set of data used for evaluation
     *
     * <p>
     * Rename this class if the name of the SampleMetadata class changes.
     * </p>
     */
    private final SampleMetadata metadata;

    /**
     * Optional Metadata describing the baseline set of data used for evaluation
     */
    private final SampleMetadata baselineMetadata;

    /**
     * Details about the project used for evaluation
     */
    private final Project project;

    /**
     * Details about the feature that the metadata belongs to
     */
    private final Feature feature;

    /**
     * The position in the order of evaluation
     */
    private final int sampleNumber;

    /**
     * Creates an OrderedSampleData instance
     * @param sampleNumber The numerical ID specifying the sample's place in the pipeline for evaluation
     * @param metadata Details concerning the constraints placed on the primary data to evaluate
     * @param baselineMetadata Details concerning the constraints placed on optional baseline data to evaluate
     * @param project Details about the project driving the evaluation
     * @param feature Details about the feature being evaluated
     */
    private OrderedSampleMetadata(
            final int sampleNumber,
            final SampleMetadata metadata,
            final SampleMetadata baselineMetadata,
            final Project project,
            final Feature feature
    )
    {
        this.sampleNumber = sampleNumber;
        this.metadata = metadata;
        this.project = project;
        this.feature = feature;
        this.baselineMetadata = baselineMetadata;
    }

    /**
     * @return The ID for the sample that this metadata will belong to
     */
    public int getSampleNumber()
    {
        return this.sampleNumber;
    }

    /**
     * @return Details concerning the constraints placed on the primary data to evaluate
     */
    public SampleMetadata getMetadata()
    {
        return metadata;
    }

    /**
     * @return Details concerning the constraints placed on optional baseline data to evaluate
     */
    public SampleMetadata getBaselineMetadata()
    {
        return baselineMetadata;
    }

    /**
     * @return Details about the feature being evaluated
     */
    public Feature getFeature()
    {
        return feature;
    }

    /**
     * @return Details about the project driving the evaluation
     */
    public Project getProject()
    {
        return project;
    }

    /**
     * @return The earliest lead duration from which values may be collected
     */
    public Duration getMinimumLead()
    {
        Duration minimum = this.getMetadata().getTimeWindow().getEarliestLeadDuration();

        // If the data needs to be scaled, but the earliest lead doesn't take that into account,
        // adjust the returned value to take entire range into consideration
        if ( this.getMetadata().hasTimeScale()
             && this.getMetadata()
                    .getTimeWindow()
                    .getEarliestLeadDuration()
                    .equals( this.getMetadata().getTimeWindow().getLatestLeadDuration() ) )
        {
            minimum = minimum.minus( this.getMetadata().getTimeScale().getPeriod() );
        }

        return minimum;
    }

    @Override
    public boolean equals( Object obj )
    {
        if (obj instanceof  OrderedSampleMetadata)
        {
            OrderedSampleMetadata other = (OrderedSampleMetadata)obj;

            return this.sampleNumber == other.sampleNumber &&
                   this.metadata.equals( other.metadata );
        }

        return false;
    }

    @Override
    public String toString()
    {
        // Should look like:
        // -- #2 - GLOO2: [-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,
        //                 -1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,PT24H,PT48H]
        return "#" + this.getSampleNumber() + " - " +
               ConfigHelper.getFeatureDescription( this.getFeature() ) + ": " +
               this.getMetadata().getTimeWindow();
    }

    /**
     * Facilitates construction of the Ordered Sample Metadata
     */
    public static class Builder
    {
        /**
         * The builder for Sample Metadata that will represent both primary and baseline data
         */
        private SampleMetadata.SampleMetadataBuilder sampleMetadataBuilder;

        /**
         * Details about the project driving the evaluation
         */
        private Project project;

        /**
         * Details about the feature being evaluated
         */
        private Feature feature;

        /**
         * An identifier for the primary variable being evaluated
         */
        private String variableIdentifier;

        /**
         * An identifier for the optional baseline variable being evaluated
         */
        private String baselineVariableIdentifier;

        /**
         * An optional description for the primary data
         */
        private String scenarioID;

        /**
         * An optional description for the optional baseline data
         */
        private String baselineScenarioID;

        /**
         * The location for which the metadata is valid
         */
        private Location geospatialID;

        /**
         * The number of the metadata
         * <p>
         *     A sampleNumber of 1 means that it is the first sample to be evaluated for that feature.
         *     A sampleNumber of 7 means that it is the 7th sample to be evaluated.
         * </p>
         */
        private int sampleNumber;

        public Builder()
        {
            this.sampleMetadataBuilder = new SampleMetadata.SampleMetadataBuilder();
        }

        public static Builder from(OrderedSampleMetadata sampleMetadata)
        {
            return new Builder().setSampleNumber( sampleMetadata.getSampleNumber() )
                                .setProject( sampleMetadata.getProject() )
                                .setFeature( sampleMetadata.getFeature() )
                                .setTimeWindow( sampleMetadata.metadata.getTimeWindow() );
        }

        public Builder setSampleNumber(final int sampleNumber)
        {
            this.sampleNumber = sampleNumber;
            return this;
        }

        public Builder setTimeWindow( final TimeWindow timeWindow)
        {
            this.sampleMetadataBuilder.setTimeWindow( timeWindow );
            return this;
        }

        public Builder setFeature(final Feature feature)
        {
            this.feature = feature;

            Float longitude = null;
            Float latitude = null;

            if (this.feature.getCoordinate() != null)
            {
                longitude = this.feature.getCoordinate().getLongitude();
                latitude = this.feature.getCoordinate().getLatitude();
            }

            this.geospatialID = Location.of(
                    this.feature.getComid(),
                    this.feature.getLocationId(),
                    longitude,
                    latitude,
                    this.feature.getGageId()
            );
            return this;
        }

        public Builder setProject(final Project project)
        {
            this.project = project;
            this.sampleMetadataBuilder.setProjectConfig( project.getProjectConfig() );
            this.sampleMetadataBuilder.setMeasurementUnit( MeasurementUnit.of( project.getDesiredMeasurementUnit() ) );
            this.scenarioID = project.getRight().getLabel();
            this.variableIdentifier = ConfigHelper.getVariableIdFromProjectConfig(
                    project.getProjectConfig(),
                    false
            );

            if ( this.project.hasBaseline())
            {
                this.baselineScenarioID = project.getBaseline().getLabel();
                this.baselineVariableIdentifier = ConfigHelper.getVariableIdFromProjectConfig(
                        project.getProjectConfig(),
                        true
                );
            }

            // Set the desired time scale: see #44539
            this.sampleMetadataBuilder.setTimeScale( project.getDesiredTimeScale() );

            return this;
        }

        public OrderedSampleMetadata build()
        {
            DatasetIdentifier datasetIdentifier = DatasetIdentifier.of(
                    this.geospatialID,
                    this.variableIdentifier,
                    this.scenarioID
            );

            this.sampleMetadataBuilder.setIdentifier( datasetIdentifier );

            SampleMetadata primaryMetadata = this.sampleMetadataBuilder.build();
            SampleMetadata baselineMetadata = null;

            if (this.project.hasBaseline())
            {
                datasetIdentifier = DatasetIdentifier.of(
                    this.geospatialID,
                    this.baselineVariableIdentifier,
                    this.baselineScenarioID
                );
                this.sampleMetadataBuilder.setIdentifier( datasetIdentifier );
                baselineMetadata = this.sampleMetadataBuilder.build();
            }

            return new OrderedSampleMetadata(
                    this.sampleNumber,
                    primaryMetadata,
                    baselineMetadata,
                    this.project,
                    this.feature
            );
        }
    }
}
