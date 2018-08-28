package wres.io.retrieval;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.metadata.TimeScale;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.time.Event;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.Response;
import wres.io.concurrency.Executor;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.retrieval.scripting.Scripter;
import wres.io.utilities.DataProvider;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.writing.PairWriter;
import wres.util.NotImplementedException;
import wres.util.TimeHelper;
import wres.util.functional.ExceptionalTriFunction;

/**
 * Created by ctubbs on 7/17/17.
 */
class InputRetriever extends WRESCallable<SampleData<?>>
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

    private long lastLead = Long.MIN_VALUE;
    private long lastBaselineLead = Long.MIN_VALUE;

    private long firstLead = Long.MAX_VALUE;
    private long firstBaselineLead = Long.MAX_VALUE;

    /**
     * The total specifications for what data to retrieve
     */
    private final ProjectDetails projectDetails;

    /**
     * The feature whose input needs to be created
     */
    private Feature feature;

    /**
     * Function used to find all left side data based on a range of dates for a specific feature
     */
    private final ExceptionalTriFunction<Feature, LocalDateTime, LocalDateTime, Collection<Double>, IOException> getLeftValues;

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

    private String rightScript;

    /**
     * A cache for all measurement unit conversions
     */
    private Map<Integer, UnitConversions.Conversion> conversionMap;

    InputRetriever ( ProjectDetails projectDetails,
                     ExceptionalTriFunction<Feature, LocalDateTime, LocalDateTime, Collection<Double>, IOException> getLeftValues )
    {
        this.projectDetails = projectDetails;
        this.getLeftValues = getLeftValues;
    }

    public void setFeature(Feature feature)
    {
        this.feature = feature;
    }

    void setLeadIteration( int leadIteration )
    {
        this.leadIteration = leadIteration;
    }

    void setIssueDatesPool( int issueDatesPool )
    {
        this.issueDatesPool = issueDatesPool;
    }

    void setClimatology(VectorOfDoubles climatology)
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
    public SampleData<?> execute() throws SQLException, IOException
    {
        if (this.projectDetails.usesGriddedData( this.projectDetails.getRight() ))
        {
            this.primaryPairs = this.createGriddedPairs( this.projectDetails.getRight() );
        }
        else
        {
            this.primaryPairs = this.createPairs(this.projectDetails.getRight());
        }

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
                if (this.projectDetails.usesGriddedData( this.projectDetails.getBaseline() ))
                {
                    this.baselinePairs = this.createGriddedPairs( this.projectDetails.getBaseline() );
                }
                else
                {
                    this.baselinePairs =
                            this.createPairs( this.projectDetails.getBaseline() );
                }
            }
        }

        SampleData<?> input;

        try
        {
            input = createInput();
        }
        catch ( IOException | SQLException e )
        {
            String message = "While calculating pairs for";

            if ( this.projectDetails.getIssuePoolingWindow() != null )
            {
                message += " sequence ";
                message += String.valueOf( this.issueDatesPool );
                message += " for";
            }

            message += " lead time ";
            message += String.valueOf( this.leadIteration );

            throw new RetrievalFailedException( message, e );
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
    private SampleData<?> createInput() throws IOException, SQLException
    {
        SampleData<?> input;

        SampleMetadata metadata =
                this.buildMetadata( this.projectDetails.getProjectConfig(), false );
        SampleMetadata baselineMetadata = null;

        if (this.primaryPairs.isEmpty())
        {
            LOGGER.debug( "Data could not be loaded for {}", metadata.getTimeWindow() );
            LOGGER.debug( "The script used was:" );
            LOGGER.debug( "{}{}", NEWLINE, this.rightScript );
            LOGGER.debug("Window: {}", this.leadIteration);
            LOGGER.debug( "Issue Date Sequence: {}", this.issueDatesPool );
            throw new NoDataException( "No data could be retrieved for Metric calculation for window " +
                                       metadata.getTimeWindow().toString() +
                                       " for " +
                                       this.projectDetails.getRightVariableName() +
                                       " at " +
                                       ConfigHelper.getFeatureDescription( this.feature ) );
        }
        else if (this.primaryPairs.size() == 1)
        {
            LOGGER.trace("There is only one pair in window {} for {} at {}",
                         metadata.getTimeWindow(),
                         this.projectDetails.getRightVariableName(),
                         ConfigHelper.getFeatureDescription( this.feature ));
        }

        if (this.projectDetails.hasBaseline())
        {
            baselineMetadata =
                    this.buildMetadata( this.projectDetails.getProjectConfig(), true );
        }

        try
        {

            if ( this.projectDetails.getRight().getType() == DatasourceType.ENSEMBLE_FORECASTS )
            {
                if ( ProjectConfigs.hasTimeSeriesMetrics(this.projectDetails.getProjectConfig()))
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
                if ( ProjectConfigs.hasTimeSeriesMetrics(this.projectDetails.getProjectConfig()))
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
        catch ( SampleDataException mie )
        {
            String message = "A collection of pairs could not be created at"
                             + " window "
                             + ( this.leadIteration + 1 )
                             + " for feature '"
                             + ConfigHelper.getFeatureDescription( this.feature )
                             + "'.";
            // Decorating with more information in our message.
            throw new SampleDataException( message, mie );
        }

        return input;
    }

    private SampleData createSingleValuedInput(SampleMetadata rightMetadata, SampleMetadata baselineMetadata)
    {
        List<SingleValuedPair> primary = convertToPairOfDoubles( this.primaryPairs );
        List<SingleValuedPair> baseline = null;

        if ( this.baselinePairs != null && !this.baselinePairs.isEmpty() )
        {
            baseline = convertToPairOfDoubles( this.baselinePairs );
        }

        return SingleValuedPairs.of( primary,
                                                baseline,
                                                rightMetadata,
                                                baselineMetadata,
                                                this.climatology );
    }

    private SampleData createSingleValuedTimeSeriesInput(SampleMetadata rightMetadata, SampleMetadata baselineMetadata)
            throws IOException, SQLException
    {
        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();

        Map<Instant, List<Event<SingleValuedPair>>> events = this.getSingleValuedEvents( primaryPairs );
        events.forEach( builder::addTimeSeriesData );
        builder.setMetadata( rightMetadata );

        if (baselinePairs != null)
        {
            events = this.getSingleValuedEvents( this.baselinePairs );
            events.forEach( builder::addTimeSeriesDataForBaseline );
            builder.setMetadataForBaseline( baselineMetadata );
        }

        builder.setClimatology( this.climatology );

        return builder.build();
    }

    private SampleData createEnsembleTimeSeriesInput(SampleMetadata rightMetadata, SampleMetadata baselineMetadata)
            throws IOException, SQLException
    {
        throw new NotImplementedException( "Ensemble Time Series Inputs cannot be created yet." );
        /*
        TimeSeriesOfEnsemblePairsBuilder builder = DataFactory.
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

    private SampleData createEnsembleInput(SampleMetadata rightMetadata, SampleMetadata baselineMetadata)
    {
        List<EnsemblePair> primary =
                InputRetriever.extractRawPairs( this.primaryPairs );


        List<EnsemblePair> baseline = null;

        if ( this.baselinePairs != null )
        {
            baseline = InputRetriever.extractRawPairs( this.baselinePairs );
        }

        return EnsemblePairs.of( primary,
                                            baseline,
                                            rightMetadata,
                                            baselineMetadata,
                                            this.climatology );
    }

    /*private Map<Instant, List<Event<PairOfDoubleAndVectorOfDoubles>>> getEnsembleEvents(List<ForecastedPair> pairs)
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
    }*/

    private Map<Instant, List<Event<SingleValuedPair>>> getSingleValuedEvents(List<ForecastedPair> pairs)
    {
        Map<Instant, List<Event<SingleValuedPair>>> events = new TreeMap<>(  );

        for (ForecastedPair pair : pairs)
        {
            if (!events.containsKey( pair.getBasisTime() ))
            {
                events.put( pair.getBasisTime(), new ArrayList<>() );
            }

            for (SingleValuedPair singleValue : pair.getSingleValuedPairs())
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
    private static List<EnsemblePair>
    extractRawPairs( List<ForecastedPair> pairPairs )
    {
        List<EnsemblePair> result = new ArrayList<>();

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
    private static List<SingleValuedPair>
    convertToPairOfDoubles( List<ForecastedPair> multiValuedPairs )
    {
        List<SingleValuedPair> pairs = new ArrayList<>(  );

        for ( ForecastedPair pair : multiValuedPairs)
        {
            for ( double pairedValue : pair.getValues().getRight() )
            {
                pairs.add( SingleValuedPair.of( pair.getValues()
                                                   .getLeft(),
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
            loadScript = Scripter.getLoadScript( this.projectDetails,
                                                 dataSourceConfig,
                                                 feature,
                                                 leadIteration,
                                                 this.issueDatesPool );

            // We save the script for debugging purposes
            this.rightScript = loadScript;
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

    private List<ForecastedPair> createGriddedPairs( DataSourceConfig dataSourceConfig )
            throws IOException, SQLException
    {
        List<ForecastedPair> pairs = new ArrayList<>(  );
        Pair<Integer, Integer>
                leadRange = this.projectDetails.getLeadRange( this.feature, this.leadIteration );

        Request griddedRequest = ConfigHelper.getGridDataRequest(
                this.projectDetails,
                dataSourceConfig,
                this.feature );

        griddedRequest.setEarliestLead( Duration.of(leadRange.getLeft(), TimeHelper.LEAD_RESOLUTION) );
        griddedRequest.setLatestLead( Duration.of(leadRange.getRight(), TimeHelper.LEAD_RESOLUTION) );

        DataSources.getSourcePaths(
                this.projectDetails,
                dataSourceConfig,
                this.issueDatesPool,
                leadRange.getLeft(),
                leadRange.getRight()
        ).forEach(griddedRequest::addPath);

        Response response = Fetcher.getData( griddedRequest );

        int minimumLead = leadRange.getLeft();
        int period = this.projectDetails.getScale().getPeriod();
        int frequency = this.projectDetails.getScale().getFrequency();

        period = (int)TimeHelper.unitsToLeadUnits(
                this.projectDetails.getScale().getUnit().value(),
                period
        );

        frequency = (int)TimeHelper.unitsToLeadUnits(
                this.projectDetails.getScale().getUnit().value(),
                frequency
        );

        for (List<Response.Series> listOfSeries : response)
        {
            // Until we support many locations per retrieval, we don't need special handling for features
            for ( Response.Series series : listOfSeries)
            {
                IngestedValueCollection ingestedValues = new IngestedValueCollection();

                for ( Response.Entry entry : series)
                {
                    IngestedValue value = new IngestedValue(
                            entry.getValidDate(),
                            entry.getMeasurements(),
                            MeasurementUnits.getMeasurementUnitID(entry.getMeasurementUnit()),
                            ( int ) TimeHelper.durationToLeadUnits( entry.getLead() ),
                            series.getIssuedDate().getEpochSecond(),
                            this.projectDetails
                    );
                    ingestedValues.add( value );

                }
                Integer aggregationStep = ingestedValues.getFirstCondensingStep(
                        period,
                        frequency,
                        minimumLead
                );

                CondensedIngestedValue condensedValue = ingestedValues.condense(
                        aggregationStep,
                        period,
                        frequency,
                        minimumLead
                );

                while (condensedValue != null)
                {
                    this.addPair( pairs, condensedValue, dataSourceConfig );
                    aggregationStep++;
                    condensedValue = ingestedValues.condense(
                            aggregationStep,
                            period,
                            frequency,
                            minimumLead
                    );
                }
            }
        }

        return pairs;
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
        String loadScript = this.getLoadScript( dataSourceConfig );

        Connection connection = null;

        IngestedValueCollection ingestedValues = new IngestedValueCollection(  );
        long reference = -1;
        int currentLead = Integer.MIN_VALUE;

        /*
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

        try
        {
            connection = Database.getConnection();
            try (DataProvider data = Database.getResults(connection, loadScript))
            {
                int minimumLead = this.projectDetails.getLeadRange( this.feature, this.leadIteration ).getLeft();
                int period = this.projectDetails.getScale().getPeriod();
                int frequency = this.projectDetails.getScale().getFrequency();

                period = ( int ) TimeHelper.unitsToLeadUnits(
                        this.projectDetails.getScale().getUnit().value(),
                        period
                );

                frequency = ( int ) TimeHelper.unitsToLeadUnits(
                        this.projectDetails.getScale().getUnit().value(),
                        frequency
                );

                while ( data.next() )
                {
                    // Results need to be ordered by the ascending basis times
                    // first, then ascending lead times. If we have already
                    // gathered at least one value and either encounter a issue
                    // time that differs from the one we just gathered or we
                    // encounter a value for the same issue time but a
                    // non-incremented lead, we want to condense our gathered
                    // values for processing and continue
                    if ( ingestedValues.size() > 0 &&
                         ( data.getLong( "basis_epoch_time" ) != reference ||
                           data.getInt( "lead" ) <= currentLead ) )
                    {
                        Integer aggregationStep = ingestedValues.getFirstCondensingStep(
                                period,
                                frequency,
                                minimumLead
                        );

                        CondensedIngestedValue condensedValue = ingestedValues.condense(
                                aggregationStep,
                                period,
                                frequency,
                                minimumLead
                        );

                        while ( condensedValue != null )
                        {
                            this.addPair( pairs, condensedValue, dataSourceConfig );
                            aggregationStep++;
                            condensedValue = ingestedValues.condense(
                                    aggregationStep,
                                    period,
                                    frequency,
                                    minimumLead
                            );
                        }

                        ingestedValues = new IngestedValueCollection();
                    }

                    reference = data.getLong( "basis_epoch_time" );
                    currentLead = data.getInt( "lead" );

                    ingestedValues.add( data, this.projectDetails );
                }

                if ( ingestedValues.size() > 0 )
                {
                    Integer aggregationStep = ingestedValues.getFirstCondensingStep(
                            period,
                            frequency,
                            minimumLead
                    );

                    CondensedIngestedValue condensedValue = ingestedValues.condense(
                            aggregationStep,
                            period,
                            frequency,
                            minimumLead
                    );

                    while ( condensedValue != null )
                    {
                        this.addPair( pairs, condensedValue, dataSourceConfig );
                        aggregationStep++;
                        condensedValue = ingestedValues.condense(
                                aggregationStep,
                                period,
                                frequency,
                                minimumLead
                        );
                    }
                }
            }
        }
        finally
        {
            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }

        return Collections.unmodifiableList( pairs );
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

        // First, store the raw results
        List<RawPersistenceRow> rawRawPersistenceValues = new ArrayList<>();

        try
        {
            connection = Database.getConnection();
            try (DataProvider data = Database.getResults( connection, loadScript ))
            {
                while ( data.next() )
                {
                    long basisEpochTimeMillis = data.getLong( VALID_DATETIME_COLUMN );
                    double value = data.getDouble( RESULT_VALUE_COLUMN );
                    int measurementUnitId = data.getInt( MEASUREMENT_ID_COLUMN );
                    RawPersistenceRow row = new RawPersistenceRow( basisEpochTimeMillis,
                                                                   value,
                                                                   measurementUnitId );
                    rawRawPersistenceValues.add( row );
                }
            }
        }
        finally
        {
            if ( connection != null )
            {
                Database.returnConnection( connection );
            }
        }

        List<RawPersistenceRow> rawPersistenceValues =
                Collections.unmodifiableList( rawRawPersistenceValues );

        // Second, analyze the results for the majority count-per-agg-window

        Duration aggDuration = ProjectConfigs.getDurationFromTimeScale( projectDetails.getScale() );
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

                EnsemblePair pair =
                        EnsemblePair.of( primaryPair.getValues()
                                                       .getLeft(),
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

        EnsemblePair pair =
                EnsemblePair.of( primaryPair.getValues()
                                               .getLeft(),
                                    aggregatedWrapped );

        return new ForecastedPair( primaryPair.getBasisTime(),
                                   primaryPair.getValidTime(),
                                   pair );
    }

    /**
     * Pairs retrieved values with their observed equivalent, adds them to the
     * overarching list of packaged pairs, and writes them to the pair output
     *
     * <p>
     *     A pair will not be added if no values were retrieved in the first
     *     place and values could not be paired.
     * </p>
     *
     * @param pairs The overarching list of pairs
     * @param condensedIngestedValue A set of values read in from the database
     * @param dataSourceConfig The configuration that is driving this pair
     *                         generation
     * @return The list of packaged pairs with a possible new pair added
     * @throws NoDataException Thrown if there wasn't enough data to determine
     * if a pair is viable
     */
    private void addPair(
            List<ForecastedPair> pairs,
            CondensedIngestedValue condensedIngestedValue,
            DataSourceConfig dataSourceConfig)
            throws IOException, SQLException
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

                writePair( condensedIngestedValue.validTime, packagedPair, dataSourceConfig );
                pairs.add( packagedPair );
            }
        }
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
    private SampleMetadata buildMetadata( ProjectConfig projectConfig,
                                    boolean isBaseline )
            throws SQLException, IOException
    {
        DataSourceConfig sourceConfig;
        if( isBaseline )
        {
            sourceConfig = projectConfig.getInputs().getBaseline();
        }
        else
        {
            sourceConfig = projectConfig.getInputs().getRight(); 
        }

        MeasurementUnit dim = MeasurementUnit.of( this.projectDetails.getDesiredMeasurementUnit());
        Float longitude = null;
        Float latitude = null;

        if (this.feature.getCoordinate() != null)
        {
            longitude = this.feature.getCoordinate().getLongitude();
            latitude = this.feature.getCoordinate().getLatitude();
        }
        final Float longitude1 = longitude;
        final Float latitude1 = latitude;

        Location geospatialIdentifier = Location.of( this.feature.getComid(), this.feature.getLocationId(), longitude1, latitude1, this.feature.getGageId() );

        // Get the variable identifier
        String variableIdentifier = ConfigHelper.getVariableIdFromProjectConfig( projectConfig, isBaseline );

        DatasetIdentifier datasetIdentifier = DatasetIdentifier.of(geospatialIdentifier,
                                                                                   variableIdentifier,
                                                                                   sourceConfig.getLabel());
        // Replicated from earlier declaration as long
        // TODO: confirm that this is really intended as the default
        Duration firstLead = Duration.ZERO;
        Duration lastLead = Duration.ZERO;

        if ( (ConfigHelper.isForecast( sourceConfig ) && !isBaseline)
                // Persistence forecast meta is based on the forecast meta
                || ConfigHelper.isPersistence( projectDetails.getProjectConfig(),
                                               sourceConfig ) )
        {
            if (ProjectConfigs.hasTimeSeriesMetrics(this.projectDetails.getProjectConfig()))
            {
                if (this.lastLead == Long.MIN_VALUE || this.firstLead == Long.MAX_VALUE)
                {
                    throw new IOException( "Valid lead times could not be "
                                           + "retrieved from the database." );
                }

                firstLead = Duration.of( this.firstLead, TimeHelper.LEAD_RESOLUTION );
                lastLead = Duration.of( this.lastLead, TimeHelper.LEAD_RESOLUTION );
            }
            else if (this.projectDetails.getProjectConfig().getPair().getLeadTimesPoolingWindow() != null)
            {
                // If a lead times pooling window, the min and max lead are bound to the parameters of the window, not
                // the values
                Pair<Integer, Integer> range = this.projectDetails.getLeadRange( this.feature, this.leadIteration );
                firstLead = Duration.of( range.getLeft(), TimeHelper.LEAD_RESOLUTION );
                firstLead = firstLead.minus( this.projectDetails.getLeadOffset( this.feature), TimeHelper.LEAD_RESOLUTION );
                lastLead = Duration.of(range.getRight(), TimeHelper.LEAD_RESOLUTION);
                lastLead = lastLead.minus( this.projectDetails.getLeadOffset( this.feature), TimeHelper.LEAD_RESOLUTION );
            }
            else
            {
                Integer offset = this.projectDetails.getLeadOffset( this.feature );

                if (offset == null)
                {
                    throw new IOException( "The last lead of the window could not "
                                           + "be determined because the offset for "
                                           + "the window could not be determined." );
                }

                Duration offsetDuration = Duration.of( offset, TimeHelper.LEAD_RESOLUTION );
                Duration windowWidth = Duration.of( this.projectDetails.getWindowWidth(),
                                                    TimeHelper.LEAD_RESOLUTION );

                ChronoUnit leadTemporalUnit = ChronoUnit.valueOf( this.projectDetails.getLeadUnit() );
                
                Duration leadFrequency = Duration.of( this.projectDetails.getLeadFrequency(), leadTemporalUnit );
                Duration leadFrequencyMultipliedByLeadIteration = leadFrequency.multipliedBy( this.leadIteration );

                lastLead = leadFrequencyMultipliedByLeadIteration.plus( windowWidth ).plus( offsetDuration );
                firstLead = lastLead;               
            }
        }
        else if (ConfigHelper.isForecast( sourceConfig ))
        {
            Duration.of( this.firstBaselineLead, TimeHelper.LEAD_RESOLUTION);
            Duration.of( this.lastBaselineLead, TimeHelper.LEAD_RESOLUTION);
        }

        TimeWindow timeWindow = ConfigHelper.getTimeWindow( this.projectDetails,
                                                            firstLead,
                                                            lastLead,
                                                            this.issueDatesPool );
        // Build the metadata
        SampleMetadataBuilder builder = new SampleMetadataBuilder().setMeasurementUnit( dim )
                                                                   .setIdentifier( datasetIdentifier )
                                                                   .setTimeWindow( timeWindow )
                                                                   .setProjectConfig( projectConfig );
        
        // Add the time-scale information to the metadata. 
        // Initially, this comes from the desiredTimeScale. 
        // TODO: when the project declaration is undefined, 
        // determine the Least Common Scale and populate the
        // metadata with that - that relies on #54415. 
        // See #44539 for an overview.        
        if ( Objects.nonNull( projectConfig.getPair() )
             && Objects.nonNull( projectConfig.getPair().getDesiredTimeScale() ) )
        {
            builder.setTimeScale( TimeScale.of( projectConfig.getPair().getDesiredTimeScale() ) );
        }

        return builder.build();                                          
    }

    private EnsemblePair getPair(CondensedIngestedValue condensedIngestedValue)
            throws IOException, SQLException
    {
        if (condensedIngestedValue.isEmpty())
        {
            throw new NoDataException( "No values could be retrieved to pair "
                                       + "with with any possible set of left "
                                       + "values." );
        }

        Double leftAggregation = this.getLeftAggregation( condensedIngestedValue.validTime );

        // If a valid value could not be retrieved (NaN is valid, so MAX_VALUE
        // is used), return null
        if (leftAggregation == null)
        {
            LOGGER.trace( "No values from the left could be retrieved to pair with the retrieved right values." );
            return null;
        }

        return EnsemblePair.of(
                leftAggregation,
                condensedIngestedValue.getAggregatedValues(
                        this.projectDetails.shouldScale(),
                        this.projectDetails.getScale().getFunction()
                )
        );
    }

    /**
     * Finds and aggregates left hand values
     * @param end The date at which the left hand values need to be aggregated to
     * @return The scaled left hand value.
     * @throws NoDataException
     */
    private Double getLeftAggregation(Instant end)
            throws SQLException, IOException
    {

        Instant firstDate;

        if (this.projectDetails.shouldScale())
        {
            firstDate = end.minus(
                    this.projectDetails.getScale().getPeriod(),
                    ChronoUnit.valueOf( this.projectDetails.getScale().getUnit().value().toUpperCase() )
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

        //List<Double> leftValues = this.getLeftValues.apply( startDate, endDate );
        Collection<Double> leftValues = this.getLeftValues.call( this.feature, startDate, endDate );

        if (leftValues == null || leftValues.isEmpty())
        {
            LOGGER.trace( "No values from the left could be retrieved to pair with the retrieved right values." );
            return null;
        }

        Double leftAggregation;

        if (this.projectDetails.shouldScale())
        {
            leftAggregation = wres.util.Collections.aggregate(
                    leftValues,
                    this.projectDetails.getScale()
                                       .getFunction()
                                       .value()
            );
        }
        else
        {
            leftAggregation = leftValues.iterator().next();
        }

        return leftAggregation;
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

            PairWriter.Builder builder = new PairWriter.Builder();
            builder = builder.setDestinationConfig( dest );
            builder = builder.setDate( date );
            builder = builder.setFeature( this.feature );
            builder = builder.setLeadIteration( this.leadIteration );
            builder = builder.setPair( pair.getValues() );
            builder = builder.setIsBaseline( isBaseline );
            builder = builder.setPoolingStep( this.issueDatesPool );
            builder = builder.setProjectDetails( this.projectDetails );
            builder = builder.setLead( (int) pair.getLeadHours() );

            Executor.submitHighPriorityTask( builder.build() );
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
        private final EnsemblePair values;

        ForecastedPair( Instant basisTime,
                        Instant validTime,
                        EnsemblePair values )
        {
            this.basisTime = basisTime;
            this.validTime = validTime;
            this.values = values;
        }

        ForecastedPair( Instant basisTime,
                        int leadHours,
                        EnsemblePair values )
        {
            this.basisTime = basisTime;
            Duration leadTime = Duration.ofHours( leadHours );
            this.validTime = basisTime.plus( leadTime );
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

        RawPersistenceRow( long millisSinceEpoch,
                                  double value,
                                  int measurementUnitId )
        {
            this.millisSinceEpoch = millisSinceEpoch;
            this.value = value;
            this.measurementUnitId = measurementUnitId;
        }

        long getMillisSinceEpoch()
        {
            return this.millisSinceEpoch;
        }

        public double getValue()
        {
            return this.value;
        }

        int getMeasurementUnitId()
        {
            return this.measurementUnitId;
        }
    }
}
