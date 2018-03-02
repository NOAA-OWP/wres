package wres.io.retrieval;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.builders.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.time.Event;
import wres.io.concurrency.Executor;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.retrieval.scripting.Scripter;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.writing.PairWriter;
import wres.util.NotImplementedException;
import wres.util.TimeHelper;

/**
 * Created by ctubbs on 7/17/17.
 */
class InputRetriever extends WRESCallable<MetricInput<?>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InputRetriever.class);

    /**
     * The iteration over lead times for this retriever
     */
    private int leadIteration;

    /**
     * The number of the issue dates pool
     */
    private int issueDatesPool;

    /**
     * The total specifications for what data to retrieve
     */
    private final ProjectDetails projectDetails;

    /**
     * The feature whose input needs to be created
     */
    private Feature feature;

    /**
     * Function used to find all left side data based on a range of dates
     */
    private final BiFunction<LocalDateTime, LocalDateTime, List<Double>> getLeftValues;

    private final Map<LocalDateTime, LocalDateTime> leftValues = null;

    /**
     * The total set of climatology data to group with the pairs
     */
    private VectorOfDoubles climatology;

    /**
     * The listing of all pairs between left and right data
     */
    private List<ForecastedPair> primaryPairs;

    /**
     * The Listing of all pairs between left and baseline data
     */
    private List<ForecastedPair> baselinePairs;

    /**
     * A cache for all measurement unit conversions
     */
    private Map<Integer, UnitConversions.Conversion> conversionMap;

    public InputRetriever ( ProjectDetails projectDetails,
                            BiFunction<LocalDateTime, LocalDateTime, List<Double>> getLeftValues )
    {
        this.projectDetails = projectDetails;
        this.getLeftValues = getLeftValues;
    }

    public void setFeature(Feature feature)
    {
        this.feature = feature;
    }

    public void setLeadIteration( int leadIteration )
    {
        this.leadIteration = leadIteration;
    }

    public void setIssueDatesPool( int issueDatesPool )
    {
        this.issueDatesPool = issueDatesPool;
    }

    public void setClimatology(VectorOfDoubles climatology)
    {
        this.climatology = climatology;
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
    private Double convertMeasurement(Double value, int measurementUnitID)
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

    @Override
    public MetricInput<?> execute() throws Exception
    {
        this.primaryPairs = this.createPairs(this.projectDetails.getRight());

        if (this.projectDetails.hasBaseline())
        {
            if ( ConfigHelper.isPersistence( this.projectDetails.getProjectConfig(),
                                             this.projectDetails.getBaseline() ) )
            {
                this.baselinePairs =
                        this.createPersistencePairs( this.projectDetails.getBaseline(),
                                                     this.primaryPairs );
            }
            else
            {
                this.baselinePairs =
                        this.createPairs( this.projectDetails.getBaseline() );
            }
        }

        MetricInput<?> input;

        try
        {
            input = createInput();
        }
        catch ( Exception error )
        {
            String message = "Error occured while calculating pairs for";

            if ( this.projectDetails.getIssuePoolingWindow() != null )
            {
                message += " sequence ";
                message += String.valueOf( this.issueDatesPool );
                message += " for";
            }

            message += " lead time ";
            message += String.valueOf( this.leadIteration );

            LOGGER.debug( message, error );
            throw error;
        }
        return input;
    }

    /**
     * Creates a MetricInput object based on the previously retrieved pairs and
     * generated metadata.
     * @return A MetricInput object used to provide a point for evaluation
     * @throws IOException
     * @throws SQLException
     */
    private MetricInput<?> createInput() throws IOException, SQLException
    {
        MetricInput<?> input;

        Metadata metadata =
                this.buildMetadata( this.projectDetails.getProjectConfig(), false );
        Metadata baselineMetadata = null;

        if (this.primaryPairs.isEmpty())
        {
            throw new NoDataException( "No data could be retrieved for Metric calculation for window " +
                                       metadata.getTimeWindow().toString() +
                                       " for " +
                                       this.projectDetails.getRightVariableName() +
                                       " at " +
                                       ConfigHelper.getFeatureDescription( this.feature ) );
        }

        if (this.projectDetails.hasBaseline())
        {
            baselineMetadata =
                    this.buildMetadata( this.projectDetails.getProjectConfig(), true );
        }

        List timeSeriesMetricConfigs = projectDetails.getProjectConfig()
                                                     .getMetrics()
                                                     .getTimeSeriesMetric();

        try
        {

            if ( this.projectDetails.getRight().getType() == DatasourceType.ENSEMBLE_FORECASTS )
            {
                if (timeSeriesMetricConfigs != null && timeSeriesMetricConfigs.size() > 0)
                {
                    input = this.createEnsembleTimeSeriesInput( metadata, baselineMetadata );
                }
                else
                {
                    input = this.createEnsembleInput( metadata, baselineMetadata );
                }
            }
            else
            {
                if (timeSeriesMetricConfigs != null && timeSeriesMetricConfigs.size() > 0)
                {
                    input = this.createSingleValuedTimeSeriesInput( metadata, baselineMetadata );
                }
                else
                {
                    input = this.createSingleValuedInput( metadata,
                                                          baselineMetadata );
                }
            }
        }
        catch ( MetricInputException mie )
        {
            String message = "A collection of pairs could not be created at"
                             + " window "
                             + ( this.leadIteration + 1 )
                             + " for feature '"
                             + ConfigHelper.getFeatureDescription( this.feature )
                             + "'.";
            // Decorating with more information in our message.
            throw new MetricInputException( message, mie );
        }

        return input;
    }

    private MetricInput createSingleValuedInput(Metadata rightMetadata, Metadata baselineMetadata)
    {
        List<PairOfDoubles> primary = convertToPairOfDoubles( this.primaryPairs );
        List<PairOfDoubles> baseline = null;

        if ( this.baselinePairs != null && !this.baselinePairs.isEmpty() )
        {
            baseline = convertToPairOfDoubles( this.baselinePairs );
        }

        return DefaultDataFactory.getInstance()
                                  .ofSingleValuedPairs( primary,
                                                        baseline,
                                                        rightMetadata,
                                                        baselineMetadata,
                                                        this.climatology );
    }

    private MetricInput createSingleValuedTimeSeriesInput(Metadata rightMetadata, Metadata baselineMetadata)
            throws IOException, SQLException
    {
        TimeSeriesOfSingleValuedPairsBuilder builder = DefaultDataFactory.getInstance()
                                                                         .ofTimeSeriesOfSingleValuedPairsBuilder();

        Map<Instant, List<Event<PairOfDoubles>>> events = this.getSingleValuedEvents( primaryPairs );
        events.entrySet().forEach( entry -> builder.addTimeSeriesData( entry.getKey(), entry.getValue() ) );
        builder.setMetadata( rightMetadata );

        if (baselinePairs != null)
        {
            events = this.getSingleValuedEvents( this.baselinePairs );
            events.entrySet().forEach( entry -> builder.addTimeSeriesDataForBaseline( entry.getKey(), entry.getValue() ) );
            builder.setMetadataForBaseline( baselineMetadata );
        }

        builder.setClimatology( this.climatology );

        return builder.build();
    }

    private MetricInput createEnsembleTimeSeriesInput(Metadata rightMetadata, Metadata baselineMetadata)
            throws IOException, SQLException
    {
        throw new NotImplementedException( "Ensemble Time Series Inputs cannot be created yet." );
        /*
        TimeSeriesOfEnsemblePairsBuilder builder = DefaultDataFactory.getInstance()
                                                                         .ofTimeSeriesOfEnsemblePairsBuilder();

        Map<Instant, List<Event<PairOfDoubleAndVectorOfDoubles>>> events = this.getEnsembleEvents( primaryPairs );
        events.entrySet().forEach( entry -> builder.addTimeSeriesData( entry.getKey(), entry.getValue() ) );
        builder.setMetadata( rightMetadata );

        if (baselinePairs != null)
        {
            events = this.getEnsembleEvents( this.baselinePairs );
            events.entrySet().forEach( entry -> builder.addTimeSeriesDataForBaseline( entry.getKey(), entry.getValue() ) );
            builder.setMetadataForBaseline( baselineMetadata );
        }

        builder.setClimatology( this.climatology );

        return builder.build();*/
    }

    private MetricInput createEnsembleInput(Metadata rightMetadata, Metadata baselineMetadata)
    {
        List<PairOfDoubleAndVectorOfDoubles> primary =
                InputRetriever.extractRawPairs( this.primaryPairs );


        List<PairOfDoubleAndVectorOfDoubles> baseline = null;

        if ( this.baselinePairs != null )
        {
            baseline = InputRetriever.extractRawPairs( this.baselinePairs );
        }

        return DefaultDataFactory.getInstance()
                                  .ofEnsemblePairs(
                                          primary,
                                          baseline,
                                          rightMetadata,
                                          baselineMetadata,
                                          this.climatology );
    }

    private Map<Instant, List<Event<PairOfDoubleAndVectorOfDoubles>>> getEnsembleEvents(List<ForecastedPair> pairs)
    {
        Map<Instant, List<Event<PairOfDoubleAndVectorOfDoubles>>> events = new TreeMap<>(  );

        for (ForecastedPair pair : pairs)
        {
            if (!events.containsKey( pair.getBasisTime() ))
            {
                events.put( pair.getBasisTime(), new ArrayList<>() );
            }

            events.get(pair.getBasisTime()).add( Event.of( pair.getValidTime(), pair.getValues() ) );
        }

        return events;
    }

    private Map<Instant, List<Event<PairOfDoubles>>> getSingleValuedEvents(List<ForecastedPair> pairs)
    {
        Map<Instant, List<Event<PairOfDoubles>>> events = new TreeMap<>(  );

        for (ForecastedPair pair : pairs)
        {
            if (!events.containsKey( pair.getBasisTime() ))
            {
                events.put( pair.getBasisTime(), new ArrayList<>() );
            }

            for (PairOfDoubles singleValue : pair.getSingleValuedPairs())
            {
                events.get(pair.getBasisTime()).add( Event.of( pair.getValidTime(), singleValue ) );
            }
        }

        return events;
    }

    /**
     * @param pairPairs A set of packaged pairs
     * @return A list of raw pairs contained within the set of packaged pairs
     */
    private static List<PairOfDoubleAndVectorOfDoubles>
    extractRawPairs( List<ForecastedPair> pairPairs )
    {
        List<PairOfDoubleAndVectorOfDoubles> result = new ArrayList<>();

        for ( ForecastedPair pair : pairPairs )
        {
            result.add( pair.getValues() );
        }

        return Collections.unmodifiableList( result );
    }

    /**
     * @param pairs A set of packaged pairs
     * @return A list of basis times from a set of packaged pairs
     */
    private static List<Instant>
    extractBasisTimes( List<ForecastedPair> pairs )
    {
        List<Instant> result = new ArrayList<>();

        for ( ForecastedPair pair : pairs )
        {
            result.add( pair.getBasisTime() );
        }

        return Collections.unmodifiableList( result );
    }

    /**
     * @param multiValuedPairs A set of packaged pairs
     * @return A list of all raw pairs converted into single valued pairs
     */
    private static List<PairOfDoubles>
    convertToPairOfDoubles( List<ForecastedPair> multiValuedPairs )
    {
        List<PairOfDoubles> pairs = new ArrayList<>(  );

        DataFactory factory = DefaultDataFactory.getInstance();

        for ( ForecastedPair pair : multiValuedPairs)
        {
            for ( double pairedValue : pair.getValues().getItemTwo() )
            {
                pairs.add( factory.pairOf( pair.getValues()
                                               .getItemOne(),
                                           pairedValue ) );
            }
        }

        return pairs;
    }

    /**
     * @param dataSourceConfig The configuration for the side of data to retrieve
     * @return A script used to load pair data
     * @throws SQLException
     * @throws IOException
     */
    private String getLoadScript(DataSourceConfig dataSourceConfig)
            throws SQLException, IOException
    {
        String loadScript;

        if ( this.projectDetails.getRight().equals(dataSourceConfig))
        {
            loadScript = Scripter.getLoadScript( this.projectDetails, dataSourceConfig, feature,
                                                 leadIteration, this.issueDatesPool );
        }
        else
        {
            if ( ConfigHelper.isPersistence( projectDetails.getProjectConfig(),
                                             dataSourceConfig ) )
            {
                // Find the data we need to form a persistence forecast: the
                // basis times from the right side.
                List<Instant> basisTimes = InputRetriever.extractBasisTimes( this.primaryPairs );
                loadScript =
                        Scripter.getPersistenceLoadScript( projectDetails,
                                                           dataSourceConfig,
                                                           this.feature,
                                                           basisTimes );
            }
            else
            {
                loadScript =
                        Scripter.getLoadScript( this.projectDetails,
                                                dataSourceConfig,
                                                this.feature,
                                                this.leadIteration,
                                                this.issueDatesPool );
            }
        }
        return loadScript;
    }

    // TODO: REFACTOR
    /**
     * Loads pairs from the database and directs them to packaged
     * @param dataSourceConfig The configuration whose pairs to retrieve
     * @return A packaged set of pair data
     * @throws SQLException
     * @throws IOException
     */
    private List<ForecastedPair> createPairs( DataSourceConfig dataSourceConfig )
            throws SQLException, IOException
    {
        List<ForecastedPair> pairs = new ArrayList<>();
        String loadScript = getLoadScript( dataSourceConfig );

        Connection connection = null;
        ResultSet resultSet = null;

        Integer scaleMember = null;
        Integer lead = null;
        // Use dummy value of MIN to avoid NPE
        Instant valueDate = Instant.MIN;

        /**
         * Maps returned values to their position in their returned array.
         *
         * Say we retrieve:
         *
         * row 1: [v1, v2, v3, v4, v5, v6]
         * row 2: [v1, v2, v3, v4, v5, v6]
         * row 3: [v1, v2, v3, v4, v5, v6]
         * row 4: [v1, v2, v3, v4, v5, v6]
         * row 5: [v1, v2, v3, v4, v5, v6]
         *
         * The mapping will become:
         *
         * {
         *     v1 : [1, 2, 3, 4, 5],
         *     v2 : [1, 2, 3, 4, 5],
         *     v3 : [1, 2, 3, 4, 5],
         *     v4 : [1, 2, 3, 4, 5],
         *     v5 : [1, 2, 3, 4, 5],
         *     v6 : [1, 2, 3, 4, 5]
         * }
         *
         * This ensures that we can aggregate all the v# values independently
         * (i.e. the mean of the v1s, the mean of the v2s, etc).
         *
         * In the end, each created pair will be of the form:
         *
         * left value : [ agg(v1), agg(v2), agg(v3), agg(v4), agg(v5), agg(v6) ]
         */
        Map<Integer, List<Double>> rightValues = new TreeMap<>();

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults(connection, loadScript);

            while(resultSet.next())
            {
                /**
                 * scale_member : The member of the scale to group into an
                 * aggregation. If the period of aggregation is six, you can
                 * have six different scale members: 0, 1, 2, 3, 4, and 5.
                 */

                // TODO: The scale_member doesn't always link basis times; see
                // scenario400, where we have the same date for lead time 2
                // for 7/27 and lead time 6 for 7/28. It tries to lump the
                // two together, which crosses basis times.
                // Consider adding an identity function that ignores the
                // scale_member or bringing/lumping together based on basis time
                // (probably a way better solution)
                //
                // See Bug #41816

                if (scaleMember != null &&
                    (!this.projectDetails.shouldScale(dataSourceConfig) || resultSet.getInt( "scale_member" ) <= scaleMember ))
                {
                    if (this.shouldAddPair( scaleMember, dataSourceConfig ))
                    {
                        pairs = this.addPair( pairs,
                                              valueDate,
                                              rightValues,
                                              dataSourceConfig,
                                              lead );
                    }
                    else
                    {
                        LOGGER.trace("A pair isn't being added for validation"
                                     + "because it represents an incomplete"
                                     + "dataset.");
                    }

                    rightValues = new TreeMap<>(  );
                }

                scaleMember = resultSet.getInt( "scale_member" );
                valueDate = Database.getInstant( resultSet, "value_date" );

                lead = Database.getValue( resultSet, "lead" );

                Double[] measurements = (Double[])resultSet.getArray("measurements").getArray();

                for (int measurementIndex = 0; measurementIndex < measurements.length; ++measurementIndex)
                {
                    Integer measurementUnitID = resultSet.getInt( "measurementunit_id" );
                    rightValues.putIfAbsent( measurementIndex, new ArrayList<>() );
                    rightValues.get(measurementIndex)
                               .add( this.convertMeasurement( measurements[measurementIndex],
                                                              measurementUnitID ) );
                }
            }

            // Organizing scaling periods is done based on a modulo operation -
            // meaning that, for a period of 6, there should be a 6 values, but
            // the last one won't have a scaleMember equalling the period. The
            // scaleMember of the last number is actually one below.  If there isn't
            // a scaling operation, we don't care.
            if ( rightValues.size() > 0 && this.shouldAddPair( scaleMember, dataSourceConfig ))
            {
                pairs = this.addPair( pairs,
                                      valueDate,
                                      rightValues,
                                      dataSourceConfig,
                                      lead );
            }
        }
        finally
        {
            if (resultSet != null)
            {
                resultSet.close();
            }

            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }

        return Collections.unmodifiableList( pairs );
    }

    /**
     * Determines whether or not a set of pairs should be added based off of the period
     * <p>
     *     A pair should be added if no scaling should occur, if the period is
     *     empty, or the member is the last possible member for the scale
     *     (i.e. member 5 of a 6 unit scale)
     * </p>
     * @param scaleMember
     * @return
     * @throws NoDataException
     */
    private boolean shouldAddPair(Integer scaleMember, DataSourceConfig dataSourceConfig)
            throws IOException
    {
        long period = TimeHelper.unitsToLeadUnits(
                this.projectDetails.getScale().getUnit().value(),
                this.projectDetails.getScale().getPeriod()
        );

        return !this.projectDetails.shouldScale(dataSourceConfig) ||
               period == 0 ||
               scaleMember == period - 1;
    }


    /**
     * Packages pairs based on persistence forecasting logic
     * @param dataSourceConfig The specification for the baseline
     * @param primaryPairs The set of primary pairs that have already been packaged
     * @return A packaged set of pairs following persistence forecast generation logic
     * @throws SQLException
     * @throws IOException
     */

    private List<ForecastedPair> createPersistencePairs( DataSourceConfig dataSourceConfig,
                                                         List<ForecastedPair> primaryPairs )
            throws SQLException, IOException
    {
        List<ForecastedPair> pairs = new ArrayList<>( primaryPairs.size() );

        String loadScript = getLoadScript( dataSourceConfig );

        final String VALID_DATETIME_COLUMN = "valid_time";
        final String RESULT_VALUE_COLUMN = "observed_value";
        final String MEASUREMENT_ID_COLUMN = "measurementunit_id";

        Connection connection = null;
        ResultSet resultSet = null;

        // First, store the raw results
        List<RawPersistenceRow> rawRawPersistenceValues = new ArrayList<>();

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults( connection, loadScript );

            while ( resultSet.next() )
            {
                long basisEpochTimeMillis = resultSet.getLong( VALID_DATETIME_COLUMN );
                double value = resultSet.getDouble( RESULT_VALUE_COLUMN );
                int measurementUnitId = resultSet.getInt( MEASUREMENT_ID_COLUMN );
                RawPersistenceRow row = new RawPersistenceRow( basisEpochTimeMillis,
                                                               value,
                                                               measurementUnitId );
                rawRawPersistenceValues.add( row );
            }
        }
        finally
        {
            if ( resultSet != null )
            {
                resultSet.close();
            }

            if ( connection != null )
            {
                Database.returnConnection( connection );
            }
        }

        List<RawPersistenceRow> rawPersistenceValues =
                Collections.unmodifiableList( rawRawPersistenceValues );

        // Second, analyze the results for the majority count-per-agg-window

        Duration aggDuration = ConfigHelper.getDurationFromTimeScale( projectDetails.getScale() );
        long aggDurationMillis = aggDuration.toMillis();

        LOGGER.trace( "Duration of aggregation: {}, in millis: {}",
                      aggDuration, aggDurationMillis );

        Map<Integer,Integer> countOfWindowsByCountInside = new HashMap<>();
        int totalWindowsCounted = 0;
        Adder adder = new Adder();

        // 2a slide a window over the data and count the number of values
        for ( int i = 0; i < rawPersistenceValues.size(); i++ )
        {
            RawPersistenceRow currentEvent = rawPersistenceValues.get( i );
            long earliestTime = currentEvent.getMillisSinceEpoch() - aggDurationMillis;
            Integer count = 0;

            LOGGER.trace( "i: {}, count: {}, earliestTime: {}",
                          i, count, earliestTime );

            for ( int j = i + 1; j < rawPersistenceValues.size(); j++ )
            {
                RawPersistenceRow possibleEndEvent = rawPersistenceValues.get( j );

                if ( possibleEndEvent.getMillisSinceEpoch() >= earliestTime )
                {
                    count++;
                }
                else
                {
                    // We are counting the number of windows with each count:
                    countOfWindowsByCountInside.merge( count, 1, adder );
                    totalWindowsCounted++;
                    break;
                }
            }
        }

        LOGGER.trace( "Instances of count, by count: {}",
                      countOfWindowsByCountInside );

        int countOfValuesInAGoodWindow = Integer.MIN_VALUE;

        for ( Map.Entry<Integer,Integer> entry : countOfWindowsByCountInside.entrySet() )
        {
            // Simple majority decision ("more than half")
            if ( entry.getValue() > totalWindowsCounted / 2 )
            {
                countOfValuesInAGoodWindow = entry.getKey();
                LOGGER.trace( "Found {} is the usual count of values in a window",
                              countOfValuesInAGoodWindow );
                break;
            }
        }

        if ( countOfValuesInAGoodWindow <= 0 )
        {
            throw new IllegalStateException( "The regularity of data in baseline could not be guessed." );
        }

        boolean shouldAggregate = true;

        if ( countOfValuesInAGoodWindow == 1 )
        {
            // There should be no need for aggregation when 1 value per window.
            shouldAggregate = false;
        }

        // Third, for each basis time, find the latest valid agg-window and agg

        for ( ForecastedPair primaryPair : primaryPairs )
        {
            ForecastedPair persistencePair;
            if ( shouldAggregate )
            {
                persistencePair =
                        getAggregatedPairFromRawPairs( primaryPair,
                                                       rawPersistenceValues,
                                                       aggDurationMillis,
                                                       countOfValuesInAGoodWindow,
                                                       projectDetails.getScale() );
            }
            else
            {
                persistencePair = getLatestPairFromRawPairs( primaryPair,
                                                             rawPersistenceValues );
            }

            this.writePair( persistencePair.getValidTime(), persistencePair, dataSourceConfig );
            pairs.add( persistencePair );
        }

        LOGGER.trace( "Returning persistence pairs: {}", pairs );
        return Collections.unmodifiableList( pairs );
    }


    /**
     * Create a persistence forecast pair from a set of raw pairs and a primary
     * pair.
     *
     * @param primaryPair the primary pair to match up to
     * @param rawPersistenceValues pairs of ( valid time in millis since epoch,
     *                                        value ) ordered latest to earliest
     * @return the persistence pair
     * @throws IllegalArgumentException when no persistence pair can be found
     */

    private ForecastedPair getLatestPairFromRawPairs( ForecastedPair primaryPair,
                                                      List<RawPersistenceRow> rawPersistenceValues )
    {
        for ( RawPersistenceRow rawPair : rawPersistenceValues )
        {
            if ( rawPair.getMillisSinceEpoch() < primaryPair.getBasisTime().toEpochMilli() )
            {
                // Convert units!
                Double convertedValue =
                        this.convertMeasurement( rawPair.getValue(),
                                                 rawPair.getMeasurementUnitId() );

                double[] wrappedValue = { convertedValue };

                PairOfDoubleAndVectorOfDoubles pair =
                        DefaultDataFactory.getInstance()
                                          .pairOf( primaryPair.getValues()
                                                              .getItemOne(),
                                                   wrappedValue );

                return new ForecastedPair( primaryPair.getBasisTime(),
                                           primaryPair.getValidTime(),
                                           pair );
            }
        }

        throw new IllegalArgumentException( "Could not find a persistence "
                                            + "forecast value for pair "
                                            + primaryPair );
    }


    /**
     * Create an aggregated persistence forecast pair from a set of raw pairs
     * @param primaryPair the forecasted pair to create a persistence pair from
     * @param rawPersistenceValues pairs of ( valid time in millis since epoch, value )
     * @param aggDurationMillis the width of an aggregation window in millis
     * @param countOfValuesInAGoodWindow the count of values in a valid window
     * @return a persistence forecasted pair
     */

    private ForecastedPair getAggregatedPairFromRawPairs( ForecastedPair primaryPair,
                                                          List<RawPersistenceRow> rawPersistenceValues,
                                                          long aggDurationMillis,
                                                          int countOfValuesInAGoodWindow,
                                                          TimeScaleConfig scaleConfig )
    {
        List<Double> valuesToAggregate = new ArrayList<>( 0 );

        long basisTimeEpochMillis = primaryPair.getBasisTime().toEpochMilli();

        for ( int i = 0; i < rawPersistenceValues.size(); i++ )
        {
            RawPersistenceRow currentEvent = rawPersistenceValues.get( i );

            // We found a possible starting value. Calculate window.
            long earliestTime = currentEvent.getMillisSinceEpoch() - aggDurationMillis;
            valuesToAggregate = new ArrayList<>( countOfValuesInAGoodWindow );

            if ( basisTimeEpochMillis > currentEvent.getMillisSinceEpoch() )
            {
                for ( int j = i; j < rawPersistenceValues.size(); j++ )
                {
                    RawPersistenceRow possibleEndEvent = rawPersistenceValues.get( j );
                    long epochMillisOfEvent = possibleEndEvent.getMillisSinceEpoch();
                    Double valueOfEvent = possibleEndEvent.getValue();

                    if ( epochMillisOfEvent > earliestTime )
                    {
                        LOGGER.trace( "Adding value {} from time {} because {} >= {}",
                                      valueOfEvent,
                                      epochMillisOfEvent,
                                      epochMillisOfEvent,
                                      earliestTime );

                        // Convert units if needed!
                        Double convertedValue =
                                this.convertMeasurement( valueOfEvent,
                                                         possibleEndEvent.getMeasurementUnitId() );
                        valuesToAggregate.add( convertedValue );
                    }
                    else
                    {
                        LOGGER.trace( "Finished with this window" );
                        break;
                    }
                }

                if ( valuesToAggregate.size() == countOfValuesInAGoodWindow )
                {
                    LOGGER.trace( "Found a good window with values {}",
                                  valuesToAggregate );
                    break;
                }
            }
        }

        // aggregate!
        double aggregated = wres.util.Collections.aggregate( valuesToAggregate,
                                                             scaleConfig.getFunction()
                                                                        .value() );

        double[] aggregatedWrapped = { aggregated };

        PairOfDoubleAndVectorOfDoubles pair =
                DefaultDataFactory.getInstance()
                                  .pairOf( primaryPair.getValues()
                                                      .getItemOne(),
                                           aggregatedWrapped );

        return new ForecastedPair( primaryPair.getBasisTime(),
                                   primaryPair.getValidTime(),
                                   pair );
    }

    /**
     * Pairs retrieved values with their observed equivalent, aggregates them
     * (if necessary), adds them to the overarching list of packaged pairs,
     * and writes them to the pair output
     *
     * <p>
     *     A pair will not be added if no values were retrieved in the first
     *     place and values could not be paired.
     * </p>
     *
     * @param pairs The overarching list of pairs
     * @param valueDate The date of the most recent value added to rightValues
     * @param rightValues A mapping of values mapped to their position in the
     *                    set of retrieved data (such as ensemble position)
     * @param dataSourceConfig The configuration that is driving this pair
     *                         generation
     * @param lead The lead time of the most recently added value
     * @return The list of packaged pairs with a possible new pair added
     * @throws NoDataException
     */
    private List<ForecastedPair> addPair( List<ForecastedPair> pairs,
                                          Instant valueDate,
                                          Map<Integer, List<Double>> rightValues,
                                          DataSourceConfig dataSourceConfig,
                                          int lead )
            throws NoDataException
    {
        if ( !rightValues.isEmpty() )
        {
            PairOfDoubleAndVectorOfDoubles pair = this.getPair( valueDate, rightValues );

            if (pair != null)
            {
                ForecastedPair pairPair = new ForecastedPair( lead,
                                                              valueDate,
                                                              pair );
                writePair( valueDate, pairPair, dataSourceConfig );
                pairs.add( pairPair );
            }
        }
        return pairs;
    }

    /**
     * Creates the metadata object containing information about the location,
     * variable, unit of measurement, lead time, and time window for the
     * eventual MetricInput object
     * @param projectConfig the project configuration
     * @param isBaseline is true to build the metadata for the baseline source, false for the left and right source
     * @return A metadata object that may be used to create a MetricInput Object
     * @throws SQLException
     * @throws IOException
     */
    private Metadata buildMetadata( ProjectConfig projectConfig,
                                    boolean isBaseline )
            throws SQLException, IOException
    {
        DataFactory dataFactory = DefaultDataFactory.getInstance();
        
        DataSourceConfig sourceConfig;
        if( isBaseline )
        {
            sourceConfig = projectConfig.getInputs().getBaseline();
        }
        else
        {
            sourceConfig = projectConfig.getInputs().getRight(); 
        }

        MetadataFactory metadataFactory = dataFactory.getMetadataFactory();
        Dimension dim = metadataFactory.getDimension( this.projectDetails.getDesiredMeasurementUnit());

        String geospatialIdentifier = ConfigHelper.getFeatureDescription(this.feature);
        // Get the variable identifier
        String variableIdentifier = ConfigHelper.getVariableIdFromProjectConfig( projectConfig, isBaseline );

        DatasetIdentifier datasetIdentifier = metadataFactory.getDatasetIdentifier(geospatialIdentifier,
                                                                                   variableIdentifier,
                                                                                   sourceConfig.getLabel());
        Double lastLead = 0.0;

        if ( ConfigHelper.isForecast( sourceConfig )
                // Persistence forecast meta is based on the forecast meta
                || ConfigHelper.isPersistence( projectDetails.getProjectConfig(),
                                               sourceConfig ) )
        {
            Integer offset = this.projectDetails.getLeadOffset( this.feature );

            if (offset == null)
            {
                throw new IOException( "The last lead of the window could not "
                                       + "be determined because the offset for "
                                       + "the window could not be determined." );
            }

            lastLead = this.leadIteration *
                       this.projectDetails.getLeadFrequency() +
                       this.projectDetails.getWindowWidth() * 1.0 +
                       offset;
        }

        TimeWindow timeWindow = ConfigHelper.getTimeWindow( this.projectDetails,
                                                            lastLead.longValue(),
                                                            this.issueDatesPool );

        return metadataFactory.getMetadata( dim,
                                            datasetIdentifier,
                                            timeWindow );
    }

    /**
     * Pairs a collection of values with their left hand counter part and performs
     * any needed aggregation
     * @param lastDate The last valid date for the data contained within rightValues
     * @param rightValues A mapping of values to where they were retrieved in
     *                    the data retrieved from the database
     * @return A raw pair that may be used to build up a MetricInput
     * @throws NoDataException
     */
    private PairOfDoubleAndVectorOfDoubles getPair( Instant lastDate,
                                                    Map<Integer, List<Double>> rightValues)
            throws NoDataException
    {
        if (rightValues == null || rightValues.isEmpty() )
        {
            throw new NoDataException( "No values could be retrieved to pair "
                                       + "with with any possible set of left "
                                       + "values." );
        }

        Instant firstDate;

        if (this.projectDetails.shouldScale())
        {
            // This works for both rolling and back-to-back because of how the grouping of scale_member works
            firstDate = lastDate.minus( this.projectDetails.getScale().getPeriod(),
                                        ChronoUnit.valueOf( this.projectDetails.getScale().getUnit().value().toUpperCase() ));
        }
        else
        {
            // If we aren't aggregating, we want a single instance instead of a range
            // If we try to grab left values based on (lastDate, lastDate],
            // we end up with no left hand values. We instead decrement a short
            // period of time prior to ensure we end up with an actual range of
            // values containing the one value
            firstDate = lastDate.minus(1L, ChronoUnit.MINUTES);
        }

        // Convert to LocalDateTime for the getLeftValues function
        LocalDateTime startDate = LocalDateTime.ofInstant( firstDate, ZoneId.of( "Z" ) );
        LocalDateTime endDate = LocalDateTime.ofInstant( lastDate, ZoneId.of( "Z" ) );

        List<Double> leftValues = this.getLeftValues.apply( startDate, endDate );

        if (leftValues == null || leftValues.isEmpty())
        {
            LOGGER.trace( "No values from the left could be retrieved to pair with the retrieved right values." );
            return null;
        }

        Double leftAggregation;

        if (this.projectDetails.shouldScale())
        {
            leftAggregation =
                wres.util.Collections.aggregate( leftValues,
                                                 this.projectDetails.getScale()
                                                                    .getFunction()
                                                                    .value() );
        }
        else
        {
            leftAggregation = leftValues.get( 0 );

            if (leftAggregation == null)
            {
                leftAggregation = Double.NaN;
            }
        }

        List<Double> validAggregations = new ArrayList<>();

        for (List<Double> values : rightValues.values())
        {
            if (this.projectDetails.shouldScale())
            {
                validAggregations.add(
                        wres.util.Collections.aggregate(
                                values,
                                this.projectDetails.getScale()
                                                   .getFunction()
                                                   .value()
                        )
                );
            }
            // If we aren't aggregating, just throw it in the collection and move on
            else
            {
                validAggregations.addAll( values );
            }
        }

        return DefaultDataFactory.getInstance().pairOf( leftAggregation,
                                                        validAggregations.toArray(
                                                                new Double[validAggregations
                                                                        .size()] ) );
    }

    /**
     * Creates a task to write pair data to a file
     * @param date The date of when the pair exists
     * @param pair Pair data that will be written
     * @param dataSourceConfig The configuration that led to the creation of the pairs
     */
    private void writePair( Instant date,
                            ForecastedPair pair,
                            DataSourceConfig dataSourceConfig )
    {
        boolean isBaseline = dataSourceConfig.equals( this.projectDetails.getBaseline() );
        List<DestinationConfig> destinationConfigs = this.projectDetails.getPairDestinations();

        for ( DestinationConfig dest : destinationConfigs )
        {
            // TODO: Since we are passing the ForecastedPair object and the ProjectDetails,
            // we can probably eliminate a lot of the arguments
            PairWriter saver = new PairWriter( dest,
                                               date,
                                               this.feature,
                                               this.leadIteration,
                                               pair.getValues(),
                                               isBaseline,
                                               this.issueDatesPool,
                                               this.projectDetails,
                                               (int) pair.getLeadHours() );
            Executor.submitHighPriorityTask( saver);
        }
    }

    @Override
    protected Logger getLogger()
    {
        return InputRetriever.LOGGER;
    }


    // TODO: Should we use this as an argument structure to pass to the PairWriter?
    private static final class ForecastedPair
    {
        private final Instant basisTime;
        private final Instant validTime;
        private final PairOfDoubleAndVectorOfDoubles values;

        ForecastedPair( Instant basisTime,
                        Instant validTime,
                        PairOfDoubleAndVectorOfDoubles values )
        {
            this.basisTime = basisTime;
            this.validTime = validTime;
            this.values = values;
        }

        ForecastedPair( Instant basisTime,
                        int leadHours,
                        PairOfDoubleAndVectorOfDoubles values )
        {
            this.basisTime = basisTime;
            Duration leadTime = Duration.ofHours( leadHours );
            this.validTime = basisTime.plus( leadTime );
            this.values = values;
        }

        ForecastedPair( int leadHours,
                        Instant validTime,
                        PairOfDoubleAndVectorOfDoubles values )
        {
            Duration leadTime = Duration.ofHours( leadHours );
            this.basisTime = validTime.minus( leadTime );
            this.validTime = validTime;
            this.values = values;
        }

        public Instant getBasisTime()
        {
            return this.basisTime;
        }

        public Instant getValidTime()
        {
            return this.validTime;
        }

        public PairOfDoubleAndVectorOfDoubles getValues()
        {
            return this.values;
        }

        public PairOfDoubles[] getSingleValuedPairs()
        {
            PairOfDoubles[] pairOfDoubles = new PairOfDoubles[this.getValues().getItemTwo().length];

            for (int i = 0; i < pairOfDoubles.length; ++i)
            {
                pairOfDoubles[i] = DefaultDataFactory.getInstance()
                                                     .pairOf( this.getValues().getItemOne(),
                                                              this.getValues().getItemTwo()[i] );
            }

            return pairOfDoubles;
        }

        public Duration getLeadDuration()
        {
            long millis = this.getValidTime().toEpochMilli()
                          - this.getBasisTime().toEpochMilli();
            return Duration.of( millis, ChronoUnit.MILLIS );
        }

        public long getLeadHours()
        {
            return getLeadDuration().toHours();
        }

        public long getLeadSeconds()
        {
            return getLeadDuration().getSeconds();
        }
    }

    private static class Adder implements BinaryOperator<Integer>
    {
        @Override
        public Integer apply( Integer first, Integer second )
        {
            return first + second;
        }
    }


    /**
     * Encapsulates a single persistence result row.
     */

    private static class RawPersistenceRow
    {
        private final long millisSinceEpoch;
        private final double value;
        private final int measurementUnitId;

        public RawPersistenceRow( long millisSinceEpoch,
                                  double value,
                                  int measurementUnitId )
        {
            this.millisSinceEpoch = millisSinceEpoch;
            this.value = value;
            this.measurementUnitId = measurementUnitId;
        }

        public long getMillisSinceEpoch()
        {
            return this.millisSinceEpoch;
        }

        public double getValue()
        {
            return this.value;
        }

        public int getMeasurementUnitId()
        {
            return this.measurementUnitId;
        }
    }
}
