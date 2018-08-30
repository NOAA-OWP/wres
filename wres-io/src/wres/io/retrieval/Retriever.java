package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.SampleMetadata;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.CalculationException;
import wres.util.TimeHelper;
import wres.util.functional.ExceptionalTriFunction;

abstract class Retriever extends WRESCallable<SampleData<?>>
{
    interface CacheRetriever
            extends ExceptionalTriFunction<Feature, LocalDateTime, LocalDateTime, Collection<Double>, IOException>{}

    private final ProjectDetails projectDetails;

    private long lastLead = Long.MIN_VALUE;
    private long lastBaselineLead = Long.MIN_VALUE;

    private long firstLead = Long.MAX_VALUE;
    private long firstBaselineLead = Long.MAX_VALUE;

    private Boolean shouldThisScale;
    private TimeScaleConfig commonScale;

    private final SharedWriterManager sharedWriterManager;

    Retriever( ProjectDetails projectDetails,
               CacheRetriever getLeftValues,
               SharedWriterManager sharedWriterManager )
    {
        this.projectDetails = projectDetails;
        this.getLeftValues = getLeftValues;
        this.sharedWriterManager = sharedWriterManager;
    }

    protected SharedWriterManager getSharedWriterManager()
    {
        return this.sharedWriterManager;
    }

    /**
     * Function used to find all left side data based on a range of dates for a specific feature
     */
    private CacheRetriever getLeftValues;

    /**
     * The listing of all pairs between left and right data
     */
    private List<ForecastedPair> primaryPairs;

    /**
     * The Listing of all pairs between left and baseline data
     */
    private List<ForecastedPair> baselinePairs;

    /**
     * The total set of climatology data to group with the pairs
     */
    private VectorOfDoubles climatology;

    /**
     * The feature whose input needs to be created
     */
    private Feature feature;

    /**
     * A cache for all measurement unit conversions
     */
    private Map<Integer, UnitConversions.Conversion> conversionMap;
    private int leadIteration;

    public void setFeature(Feature feature)
    {
        this.feature = feature;
    }

    void setLeadIteration( int leadIteration )
    {
        this.leadIteration = leadIteration;
    }

    void setClimatology(VectorOfDoubles climatology)
    {
        this.climatology = climatology;
    }

    List<ForecastedPair> getPrimaryPairs()
    {
        return this.primaryPairs;
    }

    List<ForecastedPair> getBaselinePairs()
    {
        return this.baselinePairs;
    }

    protected ProjectDetails getProjectDetails()
    {
        return this.projectDetails;
    }

    @Deprecated
    protected void setPrimaryPairs(final List<ForecastedPair> pairs)
    {
        this.primaryPairs = pairs;
    }

    protected void addPrimaryPair(final ForecastedPair pair)
    {
        if (this.primaryPairs == null)
        {
            this.primaryPairs = new ArrayList<>(  );
        }

        this.primaryPairs.add(pair);
    }

    @Deprecated
    protected void setBaselinePairs(final List<ForecastedPair> pairs)
    {
        this.baselinePairs = pairs;
    }

    protected void addBaselinePair(final ForecastedPair pair)
    {
        if (this.baselinePairs == null)
        {
            this.baselinePairs = new ArrayList<>(  );
        }

        this.baselinePairs.add(pair);
    }

    protected Feature getFeature()
    {
        return this.feature;
    }

    int getLeadIteration()
    {
        return this.leadIteration;
    }

    long getLastLead()
    {
        return this.lastLead;
    }

    long getLastBaselineLead()
    {
        return lastBaselineLead;
    }

    long getFirstlead()
    {
        return this.firstLead;
    }

    long getFirstBaselineLead()
    {
        return firstBaselineLead;
    }

    VectorOfDoubles getClimatology()
    {
        return this.climatology;
    }

    protected String getFeatureDescription()
    {
        return "'" + ConfigHelper.getFeatureDescription(this.getFeature())+  "'";
    }

    /**
     * Retrieves the unit conversion operation converting the given measurement
     * unit ID to the desired unit from the configuration
     * @param measurementUnitID The ID of the measurement unit from the database
     * @return A conversion operation converting values in the units represented
     * by the given ID to the desired unit for evaluation
     */
    private UnitConversions.Conversion getConversion(int measurementUnitID)
    {
        if (this.conversionMap == null)
        {
            this.conversionMap = new TreeMap<>(  );
        }

        if (!this.conversionMap.containsKey( measurementUnitID ))
        {
            this.conversionMap.put(measurementUnitID,
                                   UnitConversions.getConversion( measurementUnitID,
                                                                  this.projectDetails.getDesiredMeasurementUnit() ));
        }

        return this.conversionMap.get( measurementUnitID );
    }

    /**
     * Converts a value to the desired unit and applies configured constraints
     * to it
     *
     * <p>
     *     If the value is non-existent, it is set to NaN. If the value is outside
     *     of the configured minimum and maximum, it is set to NaN.
     * </p>
     * @param value The value to convert
     * @param measurementUnitID The unit of measurement that the unit is in
     * @return The measurement that fits the constraint of the measurement
     */
    protected Double convertMeasurement(Double value, int measurementUnitID)
    {
        Double convertedMeasurement;
        UnitConversions.Conversion conversion = this.getConversion( measurementUnitID );

        if (value != null && !value.isNaN() && conversion != null)
        {
            convertedMeasurement = conversion.convert( value );
        }
        else
        {
            convertedMeasurement = Double.NaN;
        }

        if (convertedMeasurement < this.projectDetails.getMinimumValue() ||
            convertedMeasurement > this.projectDetails.getMaximumValue())
        {
            convertedMeasurement = Double.NaN;
        }

        return convertedMeasurement;
    }

    protected void addPair(
            final List<ForecastedPair> pairs,
            final CondensedIngestedValue condensedIngestedValue,
            final DataSourceConfig dataSourceConfig
    ) throws RetrievalFailedException
    {
        if (!condensedIngestedValue.isEmpty())
        {
            EnsemblePair pair = this.getPair( condensedIngestedValue );


            if (pair != null)
            {
                if (this.projectDetails.getInputName( dataSourceConfig ).equals(ProjectDetails.RIGHT_MEMBER))
                {
                    this.lastLead = Math.max(this.lastLead, condensedIngestedValue.lead);
                    this.firstLead = Math.min(this.firstLead, condensedIngestedValue.lead);
                }
                else
                {
                    this.lastBaselineLead = Math.max(this.lastBaselineLead, condensedIngestedValue.lead);
                    this.firstBaselineLead = Math.min(this.firstBaselineLead, condensedIngestedValue.lead);
                }

                ForecastedPair packagedPair = new ForecastedPair(
                        condensedIngestedValue.lead,
                        condensedIngestedValue.validTime,
                        pair
                );

                writePair( this.sharedWriterManager,
                           condensedIngestedValue.validTime,
                           packagedPair,
                           dataSourceConfig );
                pairs.add( packagedPair );
            }
        }
    }

    protected EnsemblePair getPair(CondensedIngestedValue condensedIngestedValue)
            throws RetrievalFailedException
    {
        if (condensedIngestedValue.isEmpty())
        {
            throw new RetrievalFailedException( "No values could be retrieved to pair "
                                       + "with with any possible set of left "
                                       + "values." );
        }

        Double leftAggregation;
        try
        {
            leftAggregation = this.getLeftAggregation( condensedIngestedValue.validTime );
        }
        catch ( CalculationException e )
        {
            throw new RetrievalFailedException( "Left values to pair with retrieved "
                                                + "right values could not be calculated.",
                                                e );
        }

        // If a valid value could not be retrieved (NaN is valid, so MAX_VALUE
        // is used), return null
        if (leftAggregation == null)
        {
            this.getLogger().trace(
                    "No values from the left could be retrieved to pair with the retrieved right values."
            );
            return null;
        }

        return EnsemblePair.of(
                leftAggregation,
                condensedIngestedValue.getAggregatedValues(
                        this.shouldScale(),
                        this.getCommonScale().getFunction()
                )
        );
    }

    /**
     * Finds and aggregates left hand values
     * @param end The date at which the left hand values need to be aggregated to
     * @return The scaled left hand value.
     * @throws CalculationException Thrown if the left aggregated value could not be calculated
     */
    protected Double getLeftAggregation(Instant end) throws CalculationException
    {
        Instant firstDate;

        if (this.shouldScale())
        {
            firstDate = end.minus(
                    this.getCommonScale().getPeriod(),
                    ChronoUnit.valueOf( this.getCommonScale()
                                            .getUnit()
                                            .value()
                                            .toUpperCase() )
            );
        }
        else
        {
            // If we aren't aggregating, we want a single instance instead of a range
            // If we try to grab left values based on (lastDate, lastDate],
            // we end up with no left hand values. We instead decrement a short
            // period of time prior to ensure we end up with an actual range of
            // values containing the one value
            firstDate = end.minus( 1L, ChronoUnit.MINUTES );
        }

        LocalDateTime startDate = LocalDateTime.ofInstant( firstDate, ZoneId.of( "Z" ) );
        LocalDateTime endDate = LocalDateTime.ofInstant(end, ZoneId.of( "Z" ) );

        Collection<Double> leftValues = this.getControlValues( startDate, endDate );

        if (leftValues == null || leftValues.isEmpty())
        {
            this.getLogger().trace(
                    "No values from the left could be retrieved to pair with the retrieved right values."
            );
            return null;
        }

        Double leftAggregation;

        if (this.shouldScale())
        {
            leftAggregation = wres.util.Collections.aggregate(
                    leftValues,
                    commonScale.getFunction().value()
            );
        }
        else
        {
            leftAggregation = leftValues.iterator().next();
        }

        return leftAggregation;
    }

    Location getGeospatialIdentifier()
    {
        Float longitude = null;
        Float latitude = null;

        if (this.getFeature().getCoordinate() != null)
        {
            longitude = this.getFeature().getCoordinate().getLongitude();
            latitude = this.getFeature().getCoordinate().getLatitude();
        }

        return Location.of(
                this.getFeature().getComid(),
                this.getFeature().getLocationId(),
                longitude,
                latitude,
                this.getFeature().getGageId() );
    }

    protected final Collection<Double> getControlValues(final LocalDateTime start, final LocalDateTime end)
            throws RetrievalFailedException
    {
        try
        {
            return this.getLeftValues.call(this.getFeature(), start, end);
        }
        catch ( IOException e )
        {
            String message = "Left values between " +
                             TimeHelper.convertDateToString(start) +
                             " and " +
                             TimeHelper.convertDateToString( end ) +
                             " for iteration " +
                             this.getLeadIteration() +
                             " for " +
                             this.getFeatureDescription() +
                             " could not be found.";
            throw new RetrievalFailedException( message, e );
        }
    }

    protected final boolean shouldScale() throws RetrievalFailedException
    {
        if (shouldThisScale == null)
        {
            try
            {
                shouldThisScale = this.projectDetails.shouldScale();
            }
            catch ( CalculationException e )
            {
                throw new RetrievalFailedException( "Data could not be retrieved because "
                                                    + "the system could not determine if "
                                                    + "scaling should be performed.", e );
            }
        }

        return shouldThisScale;
    }

    protected final TimeScaleConfig getCommonScale() throws RetrievalFailedException
    {
        if (this.commonScale == null)
        {
            try
            {
                this.commonScale = this.getProjectDetails().getScale();
            }
            catch ( CalculationException e )
            {
                throw new RetrievalFailedException( "Data could not be performed because "
                                                    + "the system could not determine how "
                                                    + "to scale if necessessary.", e );
            }
        }

        return this.commonScale;
    }

    /**
     * Creates a task to write pair data to a file
     * @param date The date of when the pair exists
     * @param pair Pair data that will be written
     * @param dataSourceConfig The configuration that led to the creation of the pairs
     */
    abstract void writePair( SharedWriterManager sharedWriterManager,
                             Instant date,
                             ForecastedPair pair,
                             DataSourceConfig dataSourceConfig );

    protected abstract SampleMetadata buildMetadata( final ProjectConfig projectConfig, final boolean isBaseline) throws IOException;
    protected abstract SampleData<?> createInput() throws IOException;
    protected abstract String getLoadScript( final DataSourceConfig dataSourceConfig) throws SQLException, IOException;


    // TODO: Should we use this as an argument structure to pass to the PairWriter?
    protected static class ForecastedPair
    {
        private final Instant basisTime;
        private final Instant validTime;
        private final EnsemblePair values;

        ForecastedPair( Instant basisTime,
                        Instant validTime,
                        EnsemblePair values )
        {
            this.basisTime = basisTime;
            this.validTime = validTime;
            this.values = values;
        }

        ForecastedPair( int leadMinutes,
                        Instant validTime,
                        EnsemblePair values )
        {
            Duration leadTime = Duration.ofMinutes( leadMinutes );
            //Duration leadTime = Duration.ofHours( leadHours );
            this.basisTime = validTime.minus( leadTime );
            this.validTime = validTime;
            this.values = values;
        }

        Instant getBasisTime()
        {
            return this.basisTime;
        }

        Instant getValidTime()
        {
            return this.validTime;
        }

        public EnsemblePair getValues()
        {
            return this.values;
        }

        SingleValuedPair[] getSingleValuedPairs()
        {
            SingleValuedPair[] pairOfDoubles = new SingleValuedPair[this.getValues().getRight().length];

            for (int i = 0; i < pairOfDoubles.length; ++i)
            {
                pairOfDoubles[i] = SingleValuedPair.of( this.getValues().getLeft(),
                                                        this.getValues().getRight()[i] );
            }

            return pairOfDoubles;
        }

        Duration getLeadDuration()
        {
            long millis = this.getValidTime().toEpochMilli()
                          - this.getBasisTime().toEpochMilli();
            return Duration.of( millis, ChronoUnit.MILLIS );
        }

        long getLeadHours()
        {
            return getLeadDuration().toHours();
        }

        // TODO: Add function to get lead to print
        // Leads are saved and passed in TimeHelper.LEAD_RESOLUTION (minutes, currently)
        // We want to display in hours. What if a set doesn't have leads that are expressed in hours?
        // AHPS data has data in both hours and minutes. In order to print that correctly, there
        // needs to be a function to determine a) are the leads in terms of hours, if not, display minutes
    }
}
