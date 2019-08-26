package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.Response;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.MeasurementUnits;
import wres.io.project.Project;
import wres.io.retrieval.scripting.Scripter;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.NoDataException;
import wres.util.TimeHelper;

/**
 * Created by ctubbs on 7/17/17.
 */
class SampleDataRetriever extends Retriever
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDataRetriever.class);

    SampleDataRetriever( final OrderedSampleMetadata sampleMetadata,
                         final CacheRetriever getLeftValues,
                         Path outputDirectoryForPairs )
    {
        super( sampleMetadata,
               getLeftValues,
               outputDirectoryForPairs );
    }

    @Override
    public SampleData<?> execute() throws SQLException
    {
        if ( this.getProjectDetails().usesGriddedData( this.getProjectDetails().getRight() ))
        {
            this.createGriddedPairs( this.getProjectDetails().getRight() );
        }
        else
        {
            this.createPairs(this.getProjectDetails().getRight());
        }

        if (this.getProjectDetails().hasBaseline())
        {
            if ( ConfigHelper.isPersistence( this.getProjectDetails().getProjectConfig(),
                                             this.getProjectDetails().getBaseline() ) )
            {
                this.createPersistencePairs( this.getProjectDetails().getBaseline(),
                                                     this.getPrimaryPairs() );
            }
            else
            {
                if (this.getProjectDetails().usesGriddedData( this.getProjectDetails().getBaseline() ))
                {
                    this.createGriddedPairs( this.getProjectDetails().getBaseline() );
                }
                else
                {
                    this.createPairs( this.getProjectDetails().getBaseline() );
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
            String message = "While calculating pairs for " + this.getSampleMetadata();

            throw new RetrievalFailedException( message, e );
        }

        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "Finished gathering data for {}", this.getSampleMetadata() );
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

        if (this.getPrimaryPairs().isEmpty())
        {
            LOGGER.trace("There are no pairs in {}",
                         this.getSampleMetadata());
        }
        else if (this.getPrimaryPairs().size() == 1)
        {
            LOGGER.trace("There is only one pair in {}",
                         this.getSampleMetadata());
        }

        try
        {
            if ( this.getProjectDetails().getRight().getType() == DatasourceType.ENSEMBLE_FORECASTS )
            {
                input = this.createEnsembleTimeSeriesInput();
            }
            else
            {
                input = this.createSingleValuedTimeSeriesInput();
            }
        }
        catch ( SampleDataException mie )
        {
            String message = "A collection of pairs could not be created for "
                             + this.getSampleMetadata()
                             + ".";
            // Decorating with more information in our message.
            throw new SampleDataException( message, mie );
        }

        return input;
    }

    private TimeSeriesOfSingleValuedPairs createSingleValuedTimeSeriesInput()
    {
        if ( this.getFirstlead() == Long.MAX_VALUE || this.getLastLead() == Long.MIN_VALUE )
        {
            // #58149-17
            LOGGER.debug( "While retrieving data from the database for {} at time window {}, "
                          + "one or both of the first and last lead durations were unbounded.",
                          this.getSampleMetadata().getMetadata().getIdentifier(),
                          this.getSampleMetadata().getMetadata().getTimeWindow() );
        }

        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();

        List<Event<SingleValuedPair>> events = this.getSingleValuedEvents( this.getPrimaryPairs() );
        
        List<TimeSeries<SingleValuedPair>> timeSeries = Retriever.getTimeSeriesFromListOfEvents( events );
        timeSeries.forEach( builder::addTimeSeries );
        builder.setMetadata( this.getSampleMetadata().getMetadata() );

        // #67532
        Project project = this.getProjectDetails();
        
        if ( project.hasBaseline() )
        {
            events = this.getSingleValuedEvents( this.getBaselinePairs() );
            List<TimeSeries<SingleValuedPair>> timeSeriesBase = Retriever.getTimeSeriesFromListOfEvents( events );
            timeSeriesBase.forEach( builder::addTimeSeriesForBaseline );
            builder.setMetadataForBaseline( this.getSampleMetadata().getBaselineMetadata() );
        }

        builder.setClimatology( this.getClimatology() );

        return builder.build();
    }

    /**
     * @return the time-series of ensemble pairs using the state of this retriever
     */

    private TimeSeriesOfEnsemblePairs createEnsembleTimeSeriesInput()
    {
        if ( this.getFirstlead() == Long.MAX_VALUE || this.getLastLead() == Long.MIN_VALUE )
        {
            // #58149-17
            LOGGER.debug( "While retrieving data from the database for {} at time window {}, "
                          + "one or both of the first and last lead durations were unbounded.",
                          this.getSampleMetadata().getMetadata().getIdentifier(),
                          this.getSampleMetadata().getMetadata().getTimeWindow() );
        }

        TimeSeriesOfEnsemblePairsBuilder builder = new TimeSeriesOfEnsemblePairsBuilder();

        List<Event<EnsemblePair>> events = this.getPrimaryPairs();

        List<TimeSeries<EnsemblePair>> timeSeries = Retriever.getTimeSeriesFromListOfEvents( events );
        timeSeries.forEach( builder::addTimeSeries );
        builder.setMetadata( this.getSampleMetadata().getMetadata() );
         
        // #67532
        Project project = this.getProjectDetails();
        
        if ( project.hasBaseline() )
        {
            events = this.getBaselinePairs();
            List<TimeSeries<EnsemblePair>> timeSeriesBase = Retriever.getTimeSeriesFromListOfEvents( events );
            timeSeriesBase.forEach( builder::addTimeSeriesForBaseline );
            builder.setMetadataForBaseline( this.getSampleMetadata().getBaselineMetadata() );
        }

        builder.setClimatology( this.getClimatology() );

        return builder.build();
    }

    private List<Event<SingleValuedPair>> getSingleValuedEvents(List<Event<EnsemblePair>> pairs)
    {
        List<Event<SingleValuedPair>> events = new ArrayList<>(  );

        for (Event<EnsemblePair> pair : pairs)
        {
            events.addAll( Retriever.unwrapEnsembleEvent( pair ) );
        }

        return events;
    }

    /**
     * @param pairs A set of packaged pairs
     * @return A list of basis times from a set of packaged pairs
     */
    private static List<Instant>
    extractBasisTimes( List<Event<EnsemblePair>> pairs )
    {
        return pairs.stream().map( Event::getReferenceTime ).collect( Collectors.toUnmodifiableList() );
    }

    /**
     * @param dataSourceConfig The configuration for the side of data to retrieve
     * @return A script used to load pair data
     * @throws SQLException Thrown if the data needed to form the script needed to create load scripts could not be formed
     * @throws IOException Thrown if the data needed to form the script needed to create load scripts could not be formed
     */
    protected String getLoadScript(DataSourceConfig dataSourceConfig) throws IOException, SQLException
    {
        String loadScript;

        if ( this.getProjectDetails().getRight().equals(dataSourceConfig))
        {
            loadScript = Scripter.getLoadScript( this.getSampleMetadata(),
                                                 dataSourceConfig);
        }
        else
        {
            if ( ConfigHelper.isPersistence( getProjectDetails().getProjectConfig(),
                                             dataSourceConfig ) )
            {
                // Find the data we need to form a persistence forecast: the
                // basis times from the right side.
                List<Instant> basisTimes = SampleDataRetriever.extractBasisTimes( this.getPrimaryPairs() );
                loadScript =
                        Scripter.getPersistenceLoadScript( this.getSampleMetadata(),
                                                           dataSourceConfig,
                                                           basisTimes );
            }
            else
            {
                loadScript = Scripter.getLoadScript( this.getSampleMetadata(),
                                                     dataSourceConfig);
            }
        }
        return loadScript;
    }

    private void createGriddedPairs( DataSourceConfig dataSourceConfig )
    {
        Response response = this.getGriddedData( dataSourceConfig );

        for (List<Response.Series> listOfSeries : response)
        {
            // Until we support many locations per retrieval, we don't need special handling for features
            for ( Response.Series series : listOfSeries)
            {
                this.addSeriesPairs( series, dataSourceConfig);
            }
        }
    }

    private void addSeriesPairs( final Response.Series series, final DataSourceConfig dataSourceConfig)
    {
        int minimumLead = TimeHelper.durationToLead(this.getSampleMetadata().getMinimumLead());

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

        IngestedValueCollection ingestedValues = this.loadGriddedValues( series );

        Integer aggregationStep;
        PivottedValues condensedValue;

        try
        {
            aggregationStep = ingestedValues.getFirstPivotStep(
                    period,
                    frequency,
                    minimumLead
            );

            condensedValue = ingestedValues.pivot(
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
            this.addPair( condensedValue, dataSourceConfig );
            aggregationStep++;
            try
            {
                condensedValue = ingestedValues.pivot(
                        aggregationStep,
                        period,
                        frequency,
                        minimumLead
                );
            }
            catch ( NoDataException e )
            {
                // This is will never trigger; this would be thrown if the above pivot call fails
                throw new RetrievalFailedException( "There was no data available to group." );
            }
        }
    }

    private Response getGriddedData(final DataSourceConfig dataSourceConfig)
    {
        Request griddedRequest = ConfigHelper.getGridDataRequest(
                this.getProjectDetails(),
                dataSourceConfig,
                this.getFeature()
        );

        griddedRequest.setEarliestLead( this.getSampleMetadata()
                                            .getMetadata()
                                            .getTimeWindow()
                                            .getEarliestLeadDuration() );
        griddedRequest.setLatestLead( this.getSampleMetadata().getMetadata().getTimeWindow().getLatestLeadDuration() );

        try
        {
            DataSources.getSourcePaths(
                    getSampleMetadata(),
                    dataSourceConfig
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

        LOGGER.trace( "Made the following gridded data request: {}{}, which produced the following gridded data "
                      + "response: {}{}",
                      System.lineSeparator(),
                      griddedRequest,
                      System.lineSeparator(),
                      response );

        return response;
    }

    private IngestedValueCollection loadGriddedValues(Response.Series series)
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
                        TimeHelper.durationToLead( entry.getLead()),
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

        return ingestedValues;
    }

    // TODO: REFACTOR
    /**
     * Loads pairs from the database and directs them to packaged
     * @param dataSourceConfig The configuration whose pairs to retrieve
     */
    private void createPairs( DataSourceConfig dataSourceConfig )
    {
        String loadScript;

        try
        {
            loadScript = this.getLoadScript( dataSourceConfig );
        }
        catch ( SQLException | IOException e )
        {
            throw new RetrievalFailedException( "The logic used to retrieve data could not be formed.", e );
        }

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

        int minimumLead = TimeHelper.durationToLead(this.getSampleMetadata().getMinimumLead());


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

        try ( DataProvider data = new DataScripter( loadScript ).buffer())
        {
            while ( data.next() )
            {
                // TODO: Convert the if logic into its own function
                // Results need to be ordered by the ascending basis times
                // first, then ascending lead times. If we have already
                // gathered at least one value and either encounter a issue
                // time that differs from the one we just gathered or we
                // encounter a value for the same issue time but a
                // non-incremented lead, we want to pivot our gathered
                // values for processing and continue
                if ( ingestedValues.size() > 0 &&
                     ( data.getLong( "basis_epoch_time" ) != reference ||
                       data.getInt( "lead" ) <= currentLead ) )
                {
                    // TODO: Convert this logic into a function
                    int aggregationStep = ingestedValues.getFirstPivotStep(
                            period,
                            frequency,
                            minimumLead
                    );

                    PivottedValues condensedValue = ingestedValues.pivot(
                            aggregationStep,
                            period,
                            frequency,
                            minimumLead
                    );

                    while ( condensedValue != null )
                    {
                        this.addPair(condensedValue, dataSourceConfig );
                        aggregationStep++;
                        condensedValue = ingestedValues.pivot(
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
        }
        catch(NoDataException e)
        {
            throw new RetrievalFailedException( "No data could be retrieved.", e );
        }
        catch(SQLException  e)
        {
            throw new RetrievalFailedException( "An error occured while trying to retrieve data.", e );
        }

        // TODO: Create some sort of "Has Data" function for IngestedValueCollection
        if ( ingestedValues.size() > 0 )
        {
            int aggregationStep = ingestedValues.getFirstPivotStep(
                    period,
                    frequency,
                    minimumLead
            );

            PivottedValues condensedValue = ingestedValues.pivot(
                    aggregationStep,
                    period,
                    frequency,
                    minimumLead
            );

            while ( condensedValue != null )
            {
                this.addPair( condensedValue, dataSourceConfig );
                aggregationStep++;
                condensedValue = ingestedValues.pivot(
                        aggregationStep,
                        period,
                        frequency,
                        minimumLead
                );
            }
        }
    }

    // TODO: Should persistence pair retrieval end up as its own object?
    /**
     * Packages pairs based on persistence forecasting logic
     * @param dataSourceConfig The specification for the baseline
     * @param primaryPairs The set of primary pairs that have already been packaged
     * @throws RetrievalFailedException Thrown if pairs could not be formed
     */
    private void createPersistencePairs( DataSourceConfig dataSourceConfig, List<Event<EnsemblePair>> primaryPairs )
            throws RetrievalFailedException
    {
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

        // First, store the raw results in descending order based on valid time
        Collection<RawPersistenceRow> rawRawPersistenceValues;

        DataScripter persistenceScript = new DataScripter( loadScript );

        try
        {
            rawRawPersistenceValues = persistenceScript.interpret(
                    row -> new RawPersistenceRow(
                            row.getInstant(VALID_DATETIME_COLUMN),
                            row.getInstant( "earliest_time" ),
                            row.getDouble( RESULT_VALUE_COLUMN ),
                            row.getInt( MEASUREMENT_ID_COLUMN )
                    )
            );
        }
        catch ( SQLException e )
        {
            throw new RetrievalFailedException( "Persistence pairs could not be loaded.", e );
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

        double simpleMajority = totalWindowsCounted / 2.0;

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
        for ( Event<EnsemblePair> primaryPair : primaryPairs )
        {
            Event<EnsemblePair> persistencePair;
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

            this.addBaselinePair( persistencePair );
        }

        LOGGER.trace( "Returning persistence pairs: {}", this.getBaselinePairs() );
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

    private Event<EnsemblePair> getLatestPairFromRawPairs( Event<EnsemblePair> primaryPair,
                                                           List<RawPersistenceRow> rawPersistenceValues )
    {
        for ( RawPersistenceRow rawPair : rawPersistenceValues )
        {
            if ( rawPair.getValidTime().isBefore( primaryPair.getReferenceTime() ) )
            {
                // Convert units!
                Double convertedValue =
                        this.convertMeasurement( rawPair.getValue(),
                                                 rawPair.getMeasurementUnitId() );

                double[] wrappedValue = { convertedValue };

                EnsemblePair pair = EnsemblePair.of( primaryPair.getValue().getLeft(), wrappedValue );

                return Event.of( primaryPair.getReferenceTime(), primaryPair.getTime(), pair );
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

    private Event<EnsemblePair> getAggregatedPairFromRawPairs( Event<EnsemblePair> primaryPair,
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
            if ( primaryPair.getReferenceTime().isAfter( currentEvent.getValidTime() ) )
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

        EnsemblePair pair = EnsemblePair.of( primaryPair.getValue().getLeft(), aggregatedWrapped );

        return Event.of( primaryPair.getReferenceTime(), primaryPair.getTime(), pair );
    }


    @Override
    protected Logger getLogger()
    {
        return SampleDataRetriever.LOGGER;
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
