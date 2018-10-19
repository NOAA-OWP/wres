package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.metadata.DatasetIdentifier;
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
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.details.ProjectDetails;
import wres.io.retrieval.scripting.Scripter;
import wres.io.utilities.DataProvider;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.writing.pair.PairSupplier;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.CalculationException;
import wres.util.NotImplementedException;
import wres.util.TimeHelper;

/**
 * Created by ctubbs on 7/17/17.
 */
class InputRetriever extends Retriever //WRESCallable<MetricInput<?>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InputRetriever.class);

    /**
     * The number of the issue dates pool
     */
    private int issueDatesPool;

    private String rightScript;

    InputRetriever ( final ProjectDetails projectDetails,
                     final CacheRetriever getLeftValues,
                     SharedWriterManager sharedWriterManager,
                     Path outputDirectoryForPairs )
    {
        super( projectDetails,
               getLeftValues,
               sharedWriterManager,
               outputDirectoryForPairs );
    }

    void setIssueDatesPool( int issueDatesPool )
    {
        this.issueDatesPool = issueDatesPool;
    }

    @Override
    public SampleData<?> execute() throws SQLException
    {
        if ( this.getProjectDetails().usesGriddedData( this.getProjectDetails().getRight() ))
        {
            this.setPrimaryPairs( this.createGriddedPairs( this.getProjectDetails().getRight() ));
        }
        else
        {
            this.setPrimaryPairs( this.createPairs(this.getProjectDetails().getRight()));
        }

        if (this.getProjectDetails().hasBaseline())
        {
            if ( ConfigHelper.isPersistence( this.getProjectDetails().getProjectConfig(),
                                             this.getProjectDetails().getBaseline() ) )
            {
                this.setBaselinePairs(
                        this.createPersistencePairs( this.getProjectDetails().getBaseline(),
                                                     this.getPrimaryPairs(),
                                                     this.getSharedWriterManager() )
                );
            }
            else
            {
                if (this.getProjectDetails().usesGriddedData( this.getProjectDetails().getBaseline() ))
                {
                    this.setBaselinePairs( this.createGriddedPairs( this.getProjectDetails().getBaseline() ) );
                }
                else
                {
                    this.setBaselinePairs(
                            this.createPairs( this.getProjectDetails().getBaseline() )
                    );
                }
            }
        }

        SampleData<?> input;

        try
        {
            input = createInput();
        }
        catch ( IOException e )
        {
            String message = "While calculating pairs for";

            if ( this.getProjectDetails().getIssuePoolingWindow() != null )
            {
                message += " sequence ";
                message += String.valueOf( this.issueDatesPool );
                message += " for";
            }

            message += " lead time ";
            message += String.valueOf( this.getLeadIteration() );

            throw new RetrievalFailedException( message, e );
        }

        return input;
    }

    /**
     * Creates a MetricInput object based on the previously retrieved pairs and
     * generated metadata.
     * @return A MetricInput object used to provide a point for evaluation
     * @throws IOException when buildMetadata fails or no data is found
     */
    @Override
    protected SampleData<?> createInput() throws IOException
    {
        SampleData<?> input;

        SampleMetadata metadata =
                this.buildMetadata( this.getProjectDetails().getProjectConfig(), false );
        SampleMetadata baselineMetadata = null;

        if (this.getPrimaryPairs().isEmpty())
        {
            LOGGER.debug( "Data could not be loaded for {}", metadata.getTimeWindow() );
            LOGGER.debug( "The script used was:" );
            LOGGER.debug( "{}{}", NEWLINE, this.rightScript );
            LOGGER.debug("Window: {}", this.getLeadIteration());
            LOGGER.debug( "Issue Date Sequence: {}", this.issueDatesPool );
            throw new NoDataException( "No data could be retrieved for Metric calculation for window " +
                                       metadata.getTimeWindow().toString() +
                                       " for " +
                                       this.getProjectDetails().getRightVariableName() +
                                       " at " +
                                       ConfigHelper.getFeatureDescription( this.getFeature() ) );
        }
        else if (this.getPrimaryPairs().size() == 1)
        {
            LOGGER.trace("There is only one pair in window {} for {} at {}",
                         metadata.getTimeWindow(),
                         this.getProjectDetails().getRightVariableName(),
                         ConfigHelper.getFeatureDescription( this.getFeature() ));
        }

        if (this.getProjectDetails().hasBaseline())
        {
            baselineMetadata =
                    this.buildMetadata( this.getProjectDetails().getProjectConfig(), true );
        }

        try
        {

            if ( this.getProjectDetails().getRight().getType() == DatasourceType.ENSEMBLE_FORECASTS )
            {
                if ( ProjectConfigs.hasTimeSeriesMetrics(this.getProjectDetails().getProjectConfig()))
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
                if ( ProjectConfigs.hasTimeSeriesMetrics(this.getProjectDetails().getProjectConfig()))
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
                             + ( this.getLeadIteration() + 1 )
                             + " for feature '"
                             + ConfigHelper.getFeatureDescription( this.getFeature() )
                             + "'.";
            // Decorating with more information in our message.
            throw new SampleDataException( message, mie );
        }

        return input;
    }

    private SampleData createSingleValuedInput(SampleMetadata rightMetadata, SampleMetadata baselineMetadata)
    {
        List<SingleValuedPair> primary = convertToPairOfDoubles( this.getPrimaryPairs() );
        List<SingleValuedPair> baseline = null;

        if ( !this.getBaselinePairs().isEmpty() )
        {
            baseline = convertToPairOfDoubles( this.getBaselinePairs() );
        }

        return SingleValuedPairs.of( primary,
                                                baseline,
                                                rightMetadata,
                                                baselineMetadata,
                                                this.getClimatology() );
    }

    private SampleData createSingleValuedTimeSeriesInput(SampleMetadata rightMetadata, SampleMetadata baselineMetadata)
            throws RetrievalFailedException
    {
        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();

        Map<Instant, List<Event<SingleValuedPair>>> events = this.getSingleValuedEvents( this.getPrimaryPairs() );
        events.forEach( builder::addTimeSeriesData );
        builder.setMetadata( rightMetadata );

        if (!this.getBaselinePairs().isEmpty())
        {
            events = this.getSingleValuedEvents( this.getBaselinePairs() );
            events.forEach( builder::addTimeSeriesDataForBaseline );
            builder.setMetadataForBaseline( baselineMetadata );
        }

        builder.setClimatology( this.getClimatology() );

        return builder.build();
    }

    private SampleData createEnsembleTimeSeriesInput(SampleMetadata rightMetadata, SampleMetadata baselineMetadata)
            throws RetrievalFailedException
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
                InputRetriever.extractRawPairs( this.getPrimaryPairs() );


        List<EnsemblePair> baseline = null;

        if ( !this.getBaselinePairs().isEmpty() )
        {
            baseline = InputRetriever.extractRawPairs( this.getBaselinePairs() );
        }

        return EnsemblePairs.of( primary,
                                            baseline,
                                            rightMetadata,
                                            baselineMetadata,
                                            this.getClimatology() );
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
    protected String getLoadScript(DataSourceConfig dataSourceConfig)
            throws SQLException, IOException
    {
        String loadScript;

        if ( this.getProjectDetails().getRight().equals(dataSourceConfig))
        {
            loadScript = Scripter.getLoadScript( this.getProjectDetails(),
                                                 dataSourceConfig,
                                                 getFeature(),
                                                 this.getLeadIteration(),
                                                 this.issueDatesPool );

            // We save the script for debugging purposes
            this.rightScript = loadScript;
        }
        else
        {
            if ( ConfigHelper.isPersistence( getProjectDetails().getProjectConfig(),
                                             dataSourceConfig ) )
            {
                // Find the data we need to form a persistence forecast: the
                // basis times from the right side.
                List<Instant> basisTimes = InputRetriever.extractBasisTimes( this.getPrimaryPairs() );
                loadScript =
                        Scripter.getPersistenceLoadScript( getProjectDetails(),
                                                           dataSourceConfig,
                                                           this.getFeature(),
                                                           basisTimes );
            }
            else
            {
                loadScript =
                        Scripter.getLoadScript( this.getProjectDetails(),
                                                dataSourceConfig,
                                                this.getFeature(),
                                                this.getLeadIteration(),
                                                this.issueDatesPool );
            }
        }
        return loadScript;
    }

    private List<ForecastedPair> createGriddedPairs( DataSourceConfig dataSourceConfig )
            throws RetrievalFailedException
    {
        List<ForecastedPair> pairs = new ArrayList<>(  );
        Pair<Integer, Integer> leadRange;
        try
        {
            leadRange = this.getProjectDetails().getLeadRange( this.getFeature(), this.getLeadIteration() );
        }
        catch ( CalculationException e )
        {
            throw new RetrievalFailedException( "Grid pairs could not be loaded because the "
                                                + "calculation used to determine the range of "
                                                + "leads to retrieve failed.",
                                                e );
        }

        Request griddedRequest;
        try
        {
            griddedRequest = ConfigHelper.getGridDataRequest(
                    this.getProjectDetails(),
                    dataSourceConfig,
                    this.getFeature() );
        }
        catch ( SQLException e )
        {
            throw new RetrievalFailedException( "The request used to retrieve gridded data could not be formed.", e );
        }

        griddedRequest.setEarliestLead( Duration.of(leadRange.getLeft(), TimeHelper.LEAD_RESOLUTION) );
        griddedRequest.setLatestLead( Duration.of(leadRange.getRight(), TimeHelper.LEAD_RESOLUTION) );

        try
        {
            DataSources.getSourcePaths(
                    this.getProjectDetails(),
                    dataSourceConfig,
                    this.issueDatesPool,
                    leadRange.getLeft(),
                    leadRange.getRight()
            ).forEach(griddedRequest::addPath);
        }
        catch ( SQLException e )
        {
            throw new RetrievalFailedException( "The files containing the data that needs "
                                                + "to be retrieved could not be loaded.",
                                                e );
        }

        Response response;
        try
        {
            response = Fetcher.getData( griddedRequest );
        }
        catch ( IOException e )
        {
            throw new RetrievalFailedException( "Raw values could not be retrieved from gridded files.", e );
        }

        int minimumLead = leadRange.getLeft();

        int period = this.getCommonScale().getPeriod();
        int frequency = this.getCommonScale().getFrequency();

        period = (int)TimeHelper.unitsToLeadUnits(
                this.getCommonScale().getUnit().value(),
                period
        );

        frequency = (int)TimeHelper.unitsToLeadUnits(
                this.getCommonScale().getUnit().value(),
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
                    IngestedValue value;

                    try
                    {
                        value = new IngestedValue(
                                entry.getValidDate(),
                                entry.getMeasurements(),
                                MeasurementUnits.getMeasurementUnitID( entry.getMeasurementUnit()),
                                ( int ) TimeHelper.durationToLongUnits( entry.getLead(), TimeHelper.LEAD_RESOLUTION ),
                                series.getIssuedDate().getEpochSecond(),
                                this.getProjectDetails()
                        );
                    }
                    catch ( SQLException e )
                    {
                        throw new RetrievalFailedException( "The unit of measurement for retrieved "
                                                            + "values could not be identified.",
                                                            e );
                    }

                    ingestedValues.add( value );

                }

                Integer aggregationStep = null;
                CondensedIngestedValue condensedValue;
                try
                {
                    aggregationStep = ingestedValues.getFirstCondensingStep(
                            period,
                            frequency,
                            minimumLead
                    );

                    condensedValue = ingestedValues.condense(
                            aggregationStep,
                            period,
                            frequency,
                            minimumLead
                    );
                }
                catch ( NoDataException e )
                {
                    throw new RetrievalFailedException( "There was no data to retrieve.", e );
                }

                while (condensedValue != null)
                {
                    this.addPair( pairs, condensedValue, dataSourceConfig );
                    aggregationStep++;
                    try
                    {
                        condensedValue = ingestedValues.condense(
                                aggregationStep,
                                period,
                                frequency,
                                minimumLead
                        );
                    }
                    catch ( NoDataException e )
                    {
                        // This is will never trigger; this would be thrown if the above condense call fails
                        throw new RetrievalFailedException( "There was no data available to group." );
                    }
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
            throws RetrievalFailedException
    {
        List<ForecastedPair> pairs = new ArrayList<>();
        String loadScript;

        try
        {
            loadScript = this.getLoadScript( dataSourceConfig );
        }
        catch ( SQLException | IOException e )
        {
            throw new RetrievalFailedException( "The logic used to retrieve data could not be formed.", e );
        }

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
            int minimumLead;
            try
            {
                minimumLead =
                        this.getProjectDetails().getLeadRange( this.getFeature(), this.getLeadIteration() ).getLeft();
            }
            catch ( CalculationException e )
            {
                throw new RetrievalFailedException(
                        "Values could not be retrieved because the minimum lead"
                        + "used to collect them could not be calculated.", e );
            }


            int period = this.getCommonScale().getPeriod();
            int frequency = this.getCommonScale().getFrequency();

            period = ( int ) TimeHelper.unitsToLeadUnits(
                    this.getCommonScale().getUnit().value(),
                    period
            );

            frequency = ( int ) TimeHelper.unitsToLeadUnits(
                    this.getCommonScale().getUnit().value(),
                    frequency
            );

            connection = Database.getConnection();
            try (DataProvider data = Database.getResults(connection, loadScript))
            {

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

                    ingestedValues.add( data, this.getProjectDetails() );
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
        catch(NoDataException e)
        {
            throw new RetrievalFailedException( "No data could be retrieved.", e );
        }
        catch(SQLException  e)
        {
            throw new RetrievalFailedException( "An error occured while trying to retrieve data.", e );
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
                                                         List<ForecastedPair> primaryPairs,
                                                         SharedWriterManager sharedWriterManager )
            throws RetrievalFailedException
    {
        List<ForecastedPair> pairs = new ArrayList<>( primaryPairs.size() );

        String loadScript;

        try
        {
            loadScript = getLoadScript( dataSourceConfig );
        }
        catch ( SQLException | IOException e )
        {
            throw new RetrievalFailedException( "The logic used to retrieve data could not be formed.", e );
        }

        final String VALID_DATETIME_COLUMN = "valid_time";
        final String RESULT_VALUE_COLUMN = "observed_value";
        final String MEASUREMENT_ID_COLUMN = "measurementunit_id";

        Connection connection = null;

        // First, store the raw results in descending order based on valid time
        Collection<RawPersistenceRow> rawRawPersistenceValues;

        try
        {
            connection = Database.getConnection();
            try (DataProvider data = Database.getResults( connection, loadScript ))
            {
                rawRawPersistenceValues = data.interpret(
                        row -> new RawPersistenceRow(
                                row.getInstant(VALID_DATETIME_COLUMN),
                                row.getInstant( "earliest_time" ),
                                row.getDouble( RESULT_VALUE_COLUMN ),
                                row.getInt( MEASUREMENT_ID_COLUMN )
                        )
                );
            }
        }
        catch ( SQLException e )
        {
            throw new RetrievalFailedException( "Persistence pairs could not be loaded.", e );
        }
        finally
        {
            if ( connection != null )
            {
                Database.returnConnection( connection );
            }
        }

        // We don't want to be able to modify the number of elements within the collection,
        // but still want random access.  Still in descending order.
        List<RawPersistenceRow> rawPersistenceValues = new ArrayList<>(rawRawPersistenceValues);
        rawPersistenceValues = Collections.unmodifiableList( rawPersistenceValues );

        // Create a mapping of the number of records per observation "window" mapped to the number
        // of times that that number of records was encountered over the course of the retrieved
        // data set
        Map<Integer,Integer> countOfWindowsByCountInside = new HashMap<>();
        int totalWindowsCounted = 0;

        // 2a slide a window over the data and count the number of values
        for ( int i = 0; i < rawPersistenceValues.size(); i++ )
        {
            RawPersistenceRow currentEvent = rawPersistenceValues.get( i );
            Integer count = 0;

            LOGGER.trace( "i: {}, count: {}, earliestTime: {}",
                          i, count, currentEvent.earliestTime );

            for ( int j = i + 1; j < rawPersistenceValues.size(); j++ )
            {
                RawPersistenceRow possibleEndEvent = rawPersistenceValues.get( j );

                // We want to count every observation whose observation time is on or after
                // the first encountered earliest time, starting at the next row.
                //
                // For the results:
                //   Valid Time            |  Earliest Time          |  value  |
                //  "2551-03-19 00:00:00"  |  "2551-03-18 21:00:00"  |  619    |
                //  "2551-03-18 23:00:00"  |  "2551-03-18 20:00:00"  |  617    |
                //  "2551-03-18 22:00:00"  |  "2551-03-18 19:00:00"  |  613    |
                //  "2551-03-18 21:00:00"  |  "2551-03-18 18:00:00"  |  607    |
                //  "2551-03-18 20:00:00"  |  "2551-03-18 17:00:00"  |  601    |
                //  "2551-03-18 19:00:00"  |  "2551-03-18 16:00:00"  |  599    |
                //  "2551-03-18 18:00:00"  |  "2551-03-18 15:00:00"  |  593    |
                //  "2551-03-18 17:00:00"  |  "2551-03-18 14:00:00"  |  587    |
                //
                // Our first value's earliest time is: "2551-03-18 21:00:00"
                //  Now we're going to lump together everything whose valid time is at
                //  or greater than that earliest time following that first row.
                // That gives us:
                //
                //  "2551-03-18 23:00:00"  |  "2551-03-18 20:00:00"  |  617    |
                //  "2551-03-18 22:00:00"  |  "2551-03-18 19:00:00"  |  613    |
                //  "2551-03-18 21:00:00"  |  "2551-03-18 18:00:00"  |  607    |
                //
                // After that row, we end up below that earliest time so we record the
                // count and move on

                if ( possibleEndEvent.getValidTime().isAfter( currentEvent.earliestTime ) ||
                     possibleEndEvent.getValidTime().equals( currentEvent.earliestTime ) )
                {
                    count++;
                }
                else
                {
                    // Now that we've determined that we have n records between our earliest
                    // time and our last record at or after that earliest time, we increment
                    // the number of times we've reached n records by 1 within the
                    // countOfWindowsByCountInside map.
                    countOfWindowsByCountInside.merge( count, 1, (x, y) -> x + y );

                    // We increment the totalWindowsCounted variable; this will help us
                    // determine what counts occured the most often
                    totalWindowsCounted++;
                    break;
                }
            }
        }

        Duration aggDuration = ProjectConfigs.getDurationFromTimeScale( this.getCommonScale() );

        LOGGER.trace( "Duration of aggregation: {}", aggDuration );

        double simpleMajority = totalWindowsCounted / 2;

        LOGGER.trace( "Instances of count, by count: {}",
                      countOfWindowsByCountInside );


        int mostCommonFrequency = Integer.MIN_VALUE;

        // Find the frequency of values that occupy at least half of the data set
        for ( Entry<Integer,Integer> entry : countOfWindowsByCountInside.entrySet() )
        {
            if (entry.getValue() >= simpleMajority)
            {
                mostCommonFrequency = entry.getKey();
                LOGGER.trace( "Found {} is the usual count of values in a window",
                              mostCommonFrequency );
                break;
            }
        }

        // If no common frequency could be found, we cannot continue
        if ( mostCommonFrequency <= 0 )
        {
            throw new IllegalStateException( "The regularity of data in baseline could not be guessed." );
        }

        // Only aggregate when there's more than one value per window
        boolean shouldAggregate = mostCommonFrequency > 1;

        // Third, for each primary pair, we want to find the latest set of aggregated observations
        for ( ForecastedPair primaryPair : primaryPairs )
        {
            ForecastedPair persistencePair;
            if ( shouldAggregate )
            {
                persistencePair =
                        getAggregatedPairFromRawPairs( primaryPair,
                                                       rawPersistenceValues,
                                                       mostCommonFrequency,
                                                       this.getCommonScale() );
            }
            else
            {
                persistencePair = getLatestPairFromRawPairs( primaryPair,
                                                             rawPersistenceValues );
            }

            this.writePair( sharedWriterManager,
                            super.getOutputDirectoryForPairs(),
                            persistencePair,
                            dataSourceConfig );
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
            if ( rawPair.getValidTime().isBefore(primaryPair.getBasisTime()) )
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
     * @param rawPersistenceValues grouping of observed values in descending order based on observation time
     * @param countOfValuesInAGoodWindow the count of values in a valid window
     * @return a persistence forecasted pair
     */

    private ForecastedPair getAggregatedPairFromRawPairs( ForecastedPair primaryPair,
                                                          List<RawPersistenceRow> rawPersistenceValues,
                                                          int countOfValuesInAGoodWindow,
                                                          TimeScaleConfig scaleConfig )
    {
        List<Double> valuesToAggregate = new ArrayList<>( 0 );

        for ( int i = 0; i < rawPersistenceValues.size(); i++ )
        {
            // Grab an anchor value for our group of values to aggregate
            RawPersistenceRow currentEvent = rawPersistenceValues.get( i );

            valuesToAggregate = new ArrayList<>( countOfValuesInAGoodWindow );

            // We want to gather values to aggregate if our anchor happens to occur after the primary pair
            if ( primaryPair.getBasisTime().isAfter( currentEvent.getValidTime() ) )
            {
                // We want to gather all values between our anchor and the last value that happens
                // to come after the earliest time for the anchor
                for ( int j = i; j < rawPersistenceValues.size(); j++ )
                {
                    RawPersistenceRow possibleEndEvent = rawPersistenceValues.get( j );
                    Double valueOfEvent = possibleEndEvent.getValue();

                    if ( possibleEndEvent.getValidTime().isAfter( currentEvent.earliestTime ) )
                    {
                        LOGGER.trace( "Adding value {} from time {} because {} > {}",
                                      valueOfEvent,
                                      possibleEndEvent,
                                      possibleEndEvent,
                                      currentEvent.earliestTime );

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

                // If our number of values to gather has been reached, we can go ahead and exit the loop;
                // we've gathered the ideal window
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
     * Creates the metadata object containing information about the location,
     * variable, unit of measurement, lead time, and time window for the
     * eventual MetricInput object
     * @param projectConfig the project configuration
     * @param isBaseline is true to build the metadata for the baseline source, false for the left and right source
     * @return A metadata object that may be used to create a MetricInput Object
     * @throws IOException
     */
    @Override
    protected SampleMetadata buildMetadata( ProjectConfig projectConfig,
                                    boolean isBaseline )
            throws IOException
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

        MeasurementUnit dim = MeasurementUnit.of( this.getProjectDetails().getDesiredMeasurementUnit());

        // Get the variable identifier
        String variableIdentifier = ConfigHelper.getVariableIdFromProjectConfig( projectConfig, isBaseline );

        DatasetIdentifier datasetIdentifier = DatasetIdentifier.of(this.getGeospatialIdentifier(),
                                                                                   variableIdentifier,
                                                                                   sourceConfig.getLabel());
        // Replicated from earlier declaration as long
        // TODO: confirm that this is really intended as the default
        Duration firstLead = Duration.ZERO;
        Duration lastLead = Duration.ZERO;

        if ( (ConfigHelper.isForecast( sourceConfig ) && !isBaseline)
                // Persistence forecast meta is based on the forecast meta
                || ConfigHelper.isPersistence( getProjectDetails().getProjectConfig(),
                                               sourceConfig ) )
        {
            if (ProjectConfigs.hasTimeSeriesMetrics(this.getProjectDetails().getProjectConfig()))
            {
                if (this.getFirstlead() == Long.MIN_VALUE || this.getLastLead() == Long.MAX_VALUE)
                {
                    throw new IOException( "Valid lead times could not be "
                                           + "retrieved from the database." );
                }

                firstLead = Duration.of( this.getFirstlead(), TimeHelper.LEAD_RESOLUTION );
                lastLead = Duration.of( this.getLastLead(), TimeHelper.LEAD_RESOLUTION );
            }
            else if (this.getProjectDetails().getProjectConfig().getPair().getLeadTimesPoolingWindow() != null)
            {
                // If a lead times pooling window, the min and max lead are bound to the parameters of the window, not
                // the values
                Pair<Integer, Integer> range = null;
                try
                {
                    range = this.getProjectDetails().getLeadRange( this.getFeature(), this.getLeadIteration() );
                }
                catch ( CalculationException e )
                {
                    String message = "Metadata for the retrieved data could not be "
                                     + "formed because the range of leads "
                                     + "could not be calculated for iteration " +
                                     this.getLeadIteration() +
                                     " for '" +
                                     this.getFeatureDescription() +
                                     "'";
                    throw new RetrievalFailedException( message, e );
                }

                firstLead = Duration.of( range.getLeft(), TimeHelper.LEAD_RESOLUTION );
                try
                {
                    firstLead = firstLead.minus( this.getProjectDetails().getLeadOffset( this.getFeature()), TimeHelper.LEAD_RESOLUTION );
                }
                catch ( SQLException | CalculationException e )
                {
                    String message = "Metadata for the retrieved data could not be formed "
                                     + "because the earliest lead for iteration " +
                                     this.getLeadIteration() +
                                     " for '" +
                                     this.getFeatureDescription() +
                                     "' could not be determined.";
                    throw new RetrievalFailedException( message, e );
                }

                lastLead = Duration.of(range.getRight(), TimeHelper.LEAD_RESOLUTION);

                try
                {
                    lastLead = lastLead.minus( this.getProjectDetails().getLeadOffset( this.getFeature()), TimeHelper.LEAD_RESOLUTION );
                }
                catch ( SQLException | CalculationException e )
                {
                    String message = "The latest lead for iteration " +
                                     this.getLeadIteration() +
                                     " for '" +
                                     this.getFeatureDescription() +
                                     "' could not be determined.";
                    throw new RetrievalFailedException( message, e );
                }
            }
            else
            {
                Integer offset;
                try
                {
                    offset = this.getProjectDetails().getLeadOffset( this.getFeature() );
                }
                catch ( SQLException | CalculationException e )
                {
                    throw new RetrievalFailedException(
                            "Metadata for retrieved data could not be formed "
                            + "because the offset for data at " +
                            this.getFeatureDescription() +
                            " could not be evaluated.", e );
                }

                if (offset == null)
                {
                    throw new IOException( "The last lead of the window could not "
                                           + "be determined because the offset for "
                                           + "the window could not be determined." );
                }

                Duration offsetDuration = Duration.of( offset, TimeHelper.LEAD_RESOLUTION );
                Duration windowWidth = null;

                try
                {
                    windowWidth = Duration.of( this.getProjectDetails().getWindowWidth(),
                                               TimeHelper.LEAD_RESOLUTION );
                }
                catch ( CalculationException e )
                {
                    throw new RetrievalFailedException(
                            "Metadata could not be formed because the width of "
                            + "windows for this project could not be determined.",
                            e
                    );
                }

                ChronoUnit leadTemporalUnit = null;
                try
                {
                    leadTemporalUnit = ChronoUnit.valueOf( this.getProjectDetails().getLeadUnit() );
                }
                catch ( CalculationException e )
                {
                    throw new RetrievalFailedException( "Metadata could not be formed because the "
                                                        + "units used to describe leads "
                                                        + "could not be determined for this project.", e );
                }

                Duration leadFrequency;
                try
                {
                    leadFrequency = Duration.of( this.getProjectDetails().getLeadFrequency(), leadTemporalUnit );
                }
                catch ( CalculationException e )
                {
                    throw new RetrievalFailedException( "Metadata could not be formed because the "
                                                        + "frequency of leads for the overall "
                                                        + "dataset could not be determined.", e );
                }

                Duration leadFrequencyMultipliedByLeadIteration = leadFrequency.multipliedBy( this.getLeadIteration() );

                lastLead = leadFrequencyMultipliedByLeadIteration.plus( windowWidth ).plus( offsetDuration );
                firstLead = lastLead;               
            }
        }
        else if (ConfigHelper.isForecast( sourceConfig ))
        {
            firstLead = Duration.of( this.getFirstBaselineLead(), TimeHelper.LEAD_RESOLUTION);
            lastLead = Duration.of( this.getLastBaselineLead(), TimeHelper.LEAD_RESOLUTION);
        }

        TimeWindow timeWindow = ConfigHelper.getTimeWindow( this.getProjectDetails(),
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

    /**
     * Creates a task to write pair data to a file
     * @param sharedWriterManager sink for pairs, writes the pairs
     * @param outputDirectory the directory into which to write pairs
     * @param pair Pair data that will be written
     * @param dataSourceConfig The configuration that led to the creation of the pairs
     */
    @Override
    protected void writePair( SharedWriterManager sharedWriterManager,
                              Path outputDirectory,
                              ForecastedPair pair,
                              DataSourceConfig dataSourceConfig )
    {
        boolean isBaseline = dataSourceConfig.equals( this.getProjectDetails().getBaseline() );
        List<DestinationConfig> destinationConfigs = this.getProjectDetails().getPairDestinations();

        for ( DestinationConfig dest : destinationConfigs )
        {
            // TODO: Since we are passing the ForecastedPair object and the ProjectDetails,
            // we can probably eliminate a lot of the arguments

            PairSupplier pairWriter = new PairSupplier.Builder()
                    .setDestinationConfig( dest )
                    .setDate( pair.getValidTime() )
                    .setFeature( this.getFeature() )
                    .setLeadIteration( this.getLeadIteration() )
                    .setPair( pair.getValues() )
                    .setIsBaseline( isBaseline )
                    .setPoolingStep( this.issueDatesPool )
                    .setProjectDetails( this.getProjectDetails() )
                    .setLead( pair.getLeadDuration() )
                    .setOutputDirectory( outputDirectory )
                    .build();

            sharedWriterManager.accept( pairWriter );
        }
    }

    @Override
    protected Logger getLogger()
    {
        return InputRetriever.LOGGER;
    }


    /**
     * Encapsulates a single persistence result row.
     */

    private static class RawPersistenceRow
    {
        private final Instant validTime;
        private final Instant earliestTime;
        private final double value;
        private final int measurementUnitId;

        RawPersistenceRow(
                Instant validTime,
                Instant earliestTime,
                double value,
                int measurementUnitId
        )
        {
            this.validTime = validTime;
            this.earliestTime = earliestTime;
            this.value = value;
            this.measurementUnitId = measurementUnitId;
        }

        Instant getValidTime()
        {
            return this.validTime;
        }

        public double getValue()
        {
            return this.value;
        }

        int getMeasurementUnitId()
        {
            return this.measurementUnitId;
        }

        @Override
        public String toString()
        {
            String string = "Basis Time: " + this.earliestTime + ", ";
            string += "Valid Time: " + this.validTime + ", ";
            string += "Value: " + this.value;

            return string;
        }
    }
}
