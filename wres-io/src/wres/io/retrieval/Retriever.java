package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.Feature;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.Pair;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.data.caching.UnitConversions;
import wres.io.project.Project;
import wres.util.CalculationException;
import wres.util.TimeHelper;
import wres.util.functional.ExceptionalTriFunction;

abstract class Retriever extends WRESCallable<SampleData<?>>
{
    interface CacheRetriever
            extends ExceptionalTriFunction<Feature, LocalDateTime, LocalDateTime, Collection<Double>, IOException>{}

    private long lastLead = Long.MIN_VALUE;

    private long firstLead = Long.MAX_VALUE;

    private Boolean shouldThisScale;
    private DesiredTimeScaleConfig commonScale;

    private final Path outputDirectoryForPairs;

    private OrderedSampleMetadata sampleMetadata;

    private static final Logger LOGGER = LoggerFactory.getLogger( Retriever.class );
    
    /**
     * @param sampleMetadata Information about the sample data that will be retrieved
     * @param getLeftValues getter of left side data
     * @param outputDirectoryForPairs the output directory into which to write pairs
     */
    Retriever( OrderedSampleMetadata sampleMetadata,
               CacheRetriever getLeftValues,
               Path outputDirectoryForPairs )
    {
        this.sampleMetadata = sampleMetadata;
        this.getLeftValues = getLeftValues;
        this.outputDirectoryForPairs = outputDirectoryForPairs;
    }

    protected Path getOutputDirectoryForPairs()
    {
        return this.outputDirectoryForPairs;
    }

    /**
     * Function used to find all left side data based on a range of dates for a specific feature
     */
    private CacheRetriever getLeftValues;

    /**
     * The listing of all pairs between left and right data
     */
    private final List<Event<EnsemblePair>> primaryPairs = new ArrayList<>();

    /**
     * The Listing of all pairs between left and baseline data
     */
    private final List<Event<EnsemblePair>> baselinePairs = new ArrayList<>(  );

    /**
     * The total set of climatology data to group with the pairs
     */
    private VectorOfDoubles climatology;

    /**
     * A cache for all measurement unit conversions
     */
    private Map<Integer, UnitConversions.Conversion> conversionMap;

    OrderedSampleMetadata getSampleMetadata()
    {
        return this.sampleMetadata;
    }

    void setClimatology(VectorOfDoubles climatology)
    {
        this.climatology = climatology;
    }

    List<Event<EnsemblePair>> getPrimaryPairs()
    {
        return this.primaryPairs;
    }

    List<Event<EnsemblePair>> getBaselinePairs()
    {
        return this.baselinePairs;
    }

    protected Project getProjectDetails()
    {
        return this.sampleMetadata.getProject();
    }

    protected Feature getFeature()
    {
        return this.sampleMetadata.getFeature();
    }

    void addPrimaryPair(final Event<EnsemblePair> pair)
    {
        this.primaryPairs.add(pair);
    }

    void addBaselinePair(final Event<EnsemblePair> pair)
    {
        this.baselinePairs.add(pair);
    }

    long getLastLead()
    {
        return this.lastLead;
    }

    long getFirstlead()
    {
        return this.firstLead;
    }

    VectorOfDoubles getClimatology()
    {
        return this.climatology;
    }

    protected String getFeatureDescription()
    {
        return "'" + ConfigHelper.getFeatureDescription( this.getFeature() ) +  "'";
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
                                   UnitConversions.getConversion(
                                           measurementUnitID,
                                           this.getProjectDetails().getDesiredMeasurementUnit()
                                   )
            );
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
    Double convertMeasurement(Double value, int measurementUnitID)
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

        if (convertedMeasurement < this.getProjectDetails().getMinimumValue() ||
            convertedMeasurement > this.getProjectDetails().getMaximumValue())
        {
            convertedMeasurement = Double.NaN;
        }

        return convertedMeasurement;
    }

    void addPair(
            final PivottedValues pivottedValues,
            final DataSourceConfig dataSourceConfig
    ) throws RetrievalFailedException
    {
        if (!pivottedValues.isEmpty())
        {
            EnsemblePair pair = this.getPair( pivottedValues );

            if ( Retriever.isValidPair( pair ) )
            {
                if (this.getProjectDetails().getInputName( dataSourceConfig ).equals( Project.RIGHT_MEMBER))
                {
                    this.lastLead = Math.max( this.lastLead,
                                              TimeHelper.durationToLongUnits( pivottedValues.getLeadDuration(),
                                                                              TimeHelper.LEAD_RESOLUTION ) );
                    this.firstLead = Math.min( this.firstLead,
                                               TimeHelper.durationToLongUnits( pivottedValues.getLeadDuration(),
                                                                               TimeHelper.LEAD_RESOLUTION ) );
                }

                Event<EnsemblePair> packagedPair =
                        Event.of( pivottedValues.getValidTime().minus( pivottedValues.getLeadDuration() ),
                                  pivottedValues.getValidTime(),
                                  pair );

                if (this.getProjectDetails().getInputName( dataSourceConfig ).equals(Project.RIGHT_MEMBER))
                {
                    this.addPrimaryPair( packagedPair );
                }
                else
                {
                    this.addBaselinePair( packagedPair );
                }
            }
            else
            {
                LOGGER.trace( "Ignoring a pair with valid time {} and lead duration {} because "
                              + "the left value is not finite or none of the right values are "
                              + "finite: {}.",
                              pivottedValues.getValidTime(),
                              pivottedValues.getLeadDuration(),
                              pair );
            }
        }
    }

    EnsemblePair getPair(PivottedValues pivottedValues )
            throws RetrievalFailedException
    {
        if ( pivottedValues.isEmpty())
        {
            throw new RetrievalFailedException( "No values could be retrieved to pair "
                                       + "with with any possible set of left "
                                       + "values." );
        }

        Double leftAggregation;
        try
        {
            leftAggregation = this.getLeftAggregation( pivottedValues.getValidTime() );
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
                pivottedValues.getAggregatedValues(
                        this.shouldScale(),
                        this.getCommonScale().getFunction()
                )
        );
    }

    /**
     * TODO: Return a primitive rather than a possibly null wrapped double.
     * 
     * Finds and aggregates left hand values
     * @param end The date at which the left hand values need to be aggregated to
     * @return The scaled left hand value.
     * @throws CalculationException Thrown if the left aggregated value could not be calculated
     */
    private Double getLeftAggregation(Instant end) throws CalculationException
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
    

    // TODO: Return an empty collection rather than a possibly null collection
    private Collection<Double> getControlValues(final LocalDateTime start, final LocalDateTime end)
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
                             " for window " +
                             this.sampleMetadata +
                             " for " +
                             this.getFeatureDescription() +
                             " could not be found.";
            throw new RetrievalFailedException( message, e );
        }
    }

    private boolean shouldScale() throws RetrievalFailedException
    {
        if (shouldThisScale == null)
        {
            try
            {
                shouldThisScale = this.getProjectDetails().shouldScale();
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

    final DesiredTimeScaleConfig getCommonScale() throws RetrievalFailedException
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

    protected abstract SampleData<?> createInput() throws IOException;
    protected abstract String getLoadScript( final DataSourceConfig dataSourceConfig) throws SQLException, IOException;
    
    /**
     * Helper that creates one single-valued event for each right value in the ensemble pair. 
     * 
     * TODO: create a retriever for a specific type of pair, rather than using an ensemble pair as a 
     * generic container.
     */
    
    static List<Event<SingleValuedPair>> unwrapEnsembleEvent( Event<EnsemblePair> pair )
    {
        List<Event<SingleValuedPair>> eventPairs = new ArrayList<>();
        for ( double rightValue : pair.getValue().getRight() )
        {
            eventPairs.add( Event.of( pair.getReferenceTime(),
                                      pair.getTime(),
                                      SingleValuedPair.of( pair.getValue().getLeft(), rightValue ) ) );
        }
        
        return Collections.unmodifiableList( eventPairs );
    }
    
    /**
     * Attempts to compose a list of {@link TimeSeries} from a list of events.
     * 
     * TODO: replace with retrieval based around uniquely identified time-series. In the presence of duplicate events
     * whose values are different, it is impossible, by definition, to know the time-series to which a duplicate 
     * belongs; rather time-series must be composed with reference to a time-series identifier.
     *  
     * @param <T> the type of event
     * @param events the events
     * @return a best guess about the time-series composed by the events
     * @throws NullPointerException if the input is null
     */

    static <T extends Pair<?, ?>> List<TimeSeries<T>> getTimeSeriesFromListOfEvents( List<Event<T>> events )
    {
        Objects.requireNonNull( events );

        // Map the events by reference datetime
        // Place any duplicates by valid time in a separate list 
        // and call recursively until no duplicates exist
        List<Event<T>> duplicates = new ArrayList<>();
        Map<Instant, SortedSet<Event<T>>> eventsByReferenceTime = new TreeMap<>();
        List<TimeSeries<T>> returnMe = new ArrayList<>();

        // Iterate the events
        for ( Event<T> nextEvent : events )
        {
            Instant referenceTime = nextEvent.getReferenceTime();

            // Existing series
            if ( eventsByReferenceTime.containsKey( referenceTime ) )
            {
                SortedSet<Event<T>> nextSeries = eventsByReferenceTime.get( referenceTime );

                // Duplicate?
                if ( nextSeries.contains( nextEvent ) )
                {
                    duplicates.add( nextEvent );
                }
                else
                {
                    nextSeries.add( nextEvent );
                }
            }
            // New series
            else
            {
                // Sorted set that checks for times only, not values
                // In other words, a duplicate is a coincident measurement by time, not value
                SortedSet<Event<T>> container = new TreeSet<>( ( e1, e2 ) -> e1.getTime().compareTo( e2.getTime() ) );

                //Add the first value
                container.add( nextEvent );
                eventsByReferenceTime.put( referenceTime, container );
            }
        }

        // Add the time-series
        for ( Map.Entry<Instant, SortedSet<Event<T>>> nextEntry : eventsByReferenceTime.entrySet() )
        {
            returnMe.add( TimeSeries.of( nextEntry.getKey(), nextEntry.getValue() ) );
        }
        
        // Add duplicates: this will be called recursively
        // until no duplicates are left
        if( !duplicates.isEmpty() )
        {
            returnMe.addAll( Retriever.getTimeSeriesFromListOfEvents( duplicates ) );
        }

        return Collections.unmodifiableList( returnMe );
    }
    
    /**
     * Returns <code>true if the left value is finite and one or more of the right values is finite, otherwise 
     * <code>false</code>.
     * 
     * @return true if the pair is valid, otherwise false
     */
    
    private static boolean isValidPair( EnsemblePair pair )
    {
        // Pair is null
        if ( Objects.isNull( pair ) )
        {
            return false;
        }

        // Left is not finite
        if ( !Double.isFinite( pair.getLeft() ) )
        {
            return false;
        }

        // No right values are finite
        // In other words, check for the absence of any finite value
        // and reverse, because that is not a valid pair
        double[] right = pair.getRight();

        return !Arrays.stream( right ).noneMatch( Double::isFinite );
    }
    
}
