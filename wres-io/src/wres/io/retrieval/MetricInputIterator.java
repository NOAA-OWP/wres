package wres.io.retrieval;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.VectorOfDoubles;
import wres.io.concurrency.InputRetriever;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Projects;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.util.Collections;
import wres.util.ProgressMonitor;
import wres.util.Strings;

abstract class MetricInputIterator implements Iterator<Future<MetricInput<?>>>
{
    protected static final String NEWLINE = System.lineSeparator();

    // Setting the initial window number to -1 ensures that our windows are 0 indexed
    private int windowNumber = -1;
    private Integer windowCount;

    private final Feature feature;

    private final ProjectDetails projectDetails;
    private NavigableMap<String, Double> leftHandMap;
    private VectorOfDoubles climatology;
    private String earliestObservationDate;

    protected int getWindowNumber()
    {
        return this.windowNumber;
    }

    protected void incrementWindowNumber()
    {
        this.windowNumber++;
    }

    protected Integer getWindowCount() throws NoDataException, SQLException,
            InvalidPropertiesFormatException
    {
        if (this.windowCount == null)
        {
            this.windowCount = this.calculateWindowCount();
        }

        return this.windowCount;
    }

    protected Feature getFeature()
    {
        return this.feature;
    }

    protected ProjectDetails getProjectDetails()
    {
        return this.projectDetails;
    }

    protected void addLeftHandValue(String date, Double measurement)
    {
        if (this.leftHandMap == null)
        {
            this.leftHandMap = new TreeMap<>(  );
        }

        this.leftHandMap.put( date, measurement );
    }

    protected VectorOfDoubles getClimatology() throws SQLException
    {
        if (this.getProjectDetails().usesProbabilityThresholds() && this.climatology == null)
        {
            /**
             * This should generate a script of the form:
             *
             * SELECT O.observed_value,
             *     O.measurementunit_id,
             *     D.member_number,
             *     COUNT(*) OVER (PARTITION BY D.member_number) AS member_count
             * FROM wres.Observation O
             * RIGHT JOIN (
             *     SELECT
             *         '1980-10-01 12:00:00'::timestamp without time zone + ( member_number || ' hours')::INTERVAL AS start_time,
             *         '1980-10-01 12:00:00'::timestamp without time zone + ( (member_number + 24) || ' hours')::INTERVAL AS end_time,
             *         member_number
             *     FROM generate_series(0, 262945, 24) AS member_number
             *     ) AS D
             *     ON D.start_time <= O.observation_time
             *         AND D.end_time > O.observation_time
             * WHERE O.variableposition_id = 1
             *     AND EXISTS (
             *       SELECT 1
             *       FROM wres.ProjectSource PS
             *       WHERE PS.project_id = 1
             *           AND PS.member = 'left'
             *           AND PS.source_id = O.source_id
             *           AND PS.inactive_time IS NULL
             *     )
             * ORDER BY member_count DESC, member_number;
             *
             * The results should look like:
             *
             * 39.3299  3  0       24
             * 39.3299  3  0       24
             * .
             * .
             * 46.3497  3  262944  1
             *
             * We will aggregate values grouped by their member number and
             * stop when we either hit the bottom of the result set or
             * hit a member count less than what we've been working with.
             * A lower number indicates a partial set, which we can't aggregate.
             * The '1' on the last example shows that there is only 1 value
             * that is a member of that group. The highest number shows that
             * there should be 24. As a result, we stop; no more full groups to
             * aggregate.
             *
             */

            StringBuilder script = new StringBuilder(  );
            script.append("SELECT O.observed_value,").append(NEWLINE);
            script.append("    O.measurementunit_id,").append(NEWLINE);
            script.append("    D.member_number,").append(NEWLINE);
            script.append("    (COUNT(*) OVER (PARTITION BY D.member_number))::int AS member_count").append(NEWLINE);
            script.append("FROM wres.Observation O").append(NEWLINE);
            script.append("RIGHT JOIN (").append(NEWLINE);
            script.append("    SELECT").append(NEWLINE);
            script.append("        ")
                  .append(this.getEarliestObservationDate()).append("::timestamp without time zone ")
                  .append("+ ( member_number || ' ").append(this.getProjectDetails().getAggregationUnit()).append("')::INTERVAL AS start_time,")
                  .append(NEWLINE);
            script.append("        ")
                  .append(this.getEarliestObservationDate()).append( "::timestamp without time zone ")
                  .append("+ ((member_number + ")
                  .append(this.getProjectDetails().getAggregationPeriod())
                  .append(" ) || ' ")
                  .append(this.getProjectDetails().getAggregationUnit())
                  .append("')::INTERVAL AS end_time,")
                  .append(NEWLINE);
            script.append("        ").append("member_number").append(NEWLINE);
            script.append("    FROM generate_series(0, ")
                  .append(ConfigHelper.getValueCount( this.getProjectDetails(),
                                                      this.getProjectDetails().getLeft(),
                                                      this.getFeature() ))
                  .append(" * ")
                  .append(this.getProjectDetails().getAggregationPeriod())
                  .append(", ")
                  .append(this.getProjectDetails().getAggregationPeriod())
                  .append(") AS member_number")
                  .append(NEWLINE);
            script.append("    ) AS D").append(NEWLINE);
            script.append("ON D.start_time <= O.observation_time").append(NEWLINE);
            script.append("    AND D.end_time > O.observation_time").append(NEWLINE);
            script.append("WHERE ")
                  .append(ConfigHelper.getVariablePositionClause( this.getFeature(),
                                                                  this.getProjectDetails().getLeftVariableID(),
                                                                  "O" ))
                  .append(NEWLINE);
            script.append("    AND EXISTS (").append(NEWLINE);
            script.append("        SELECT 1").append(NEWLINE);
            script.append("        FROM wres.ProjectSource PS").append(NEWLINE);
            script.append("        WHERE PS.project_id = ").append(this.getProjectDetails().getId()).append(NEWLINE);
            script.append("            AND PS.member = 'left'").append(NEWLINE);
            script.append("            AND PS.source_id = O.source_id").append(NEWLINE);
            script.append("            AND PS.inactive_time IS NULL").append(NEWLINE);
            script.append("    )").append(NEWLINE);
            script.append("ORDER BY member_number;");

            Connection connection = null;
            ResultSet results = null;

            try
            {
                connection = Database.getConnection();
                results = Database.getResults( connection, script.toString() );

                int memberNumber = -1;
                int maxMemberCount = -1;

                List<Double> awaitingAggregations = new ArrayList<>(  );
                List<Double> aggregatedValues = new ArrayList<>();
                Map<Integer, UnitConversions.Conversion> conversions = new TreeMap<>(  );

                while (results.next())
                {
                    if (maxMemberCount < 0)
                    {
                        maxMemberCount = Database.getValue( results, "member_count" );
                    }
                    else if ((int)(Database.getValue(results, "member_number")) > memberNumber)
                    {
                        //TODO: This logic block is used twice. It probably needs to be extracted.
                        if (maxMemberCount == awaitingAggregations.size())
                        {
                            Double aggregation = Collections.aggregate(
                                    awaitingAggregations,
                                    this.getProjectDetails()
                                        .getAggregationFunction() );

                            if ( !aggregation.isNaN() )
                            {
                                aggregatedValues.add( aggregation );
                            }
                            else
                            {
                                this.getLogger().trace( "The values for member " +
                                                        "{} could not be added " +
                                                        "to the climatology; " +
                                                        "their aggregation was NaN",
                                                        memberNumber );
                            }
                        }
                        else
                        {
                            this.getLogger().trace("The values for member {} " +
                                                  "could not be aggregated " +
                                                  "because not enough valid " +
                                                  "member values could be " +
                                                  "retrieved.",
                                                  memberNumber);
                        }

                        awaitingAggregations.clear();

                        memberNumber = Database.getValue( results, "member_number" );
                    }

                    Double observedValue = Database.getValue(results, "observed_value");

                    if (observedValue == null)
                    {
                        this.getLogger().trace( "A null value was encountered " +
                                               "from member {}. Skipping to next value...",
                                               memberNumber );
                        continue;
                    }

                    int measurementUnitID = Database.getValue( results, "measurementunit_id" );

                    if (!conversions.containsKey( measurementUnitID ))
                    {
                        conversions.put(measurementUnitID,
                                        UnitConversions.getConversion( measurementUnitID,
                                                                       this.getProjectDetails().getDesiredMeasurementUnit() ));
                    }

                    Double convertedValue = conversions.get(measurementUnitID).convert( observedValue );

                    if (convertedValue != null &&
                        !convertedValue.isNaN()  )
                    {
                        awaitingAggregations.add( conversions.get(
                                measurementUnitID ).convert( observedValue ) );
                    }
                    else
                    {
                        this.getLogger().trace( "A value could not be added to " +
                                               "member {} because it was marked " +
                                               "as an invalid value.",
                                               memberNumber );
                    }

                }

                if (maxMemberCount == awaitingAggregations.size())
                {
                    Double aggregation = Collections.aggregate(
                            awaitingAggregations,
                            this.getProjectDetails()
                                .getAggregationFunction() );

                    if ( !aggregation.isNaN() )
                    {
                        aggregatedValues.add( aggregation );
                    }
                }
                else
                {
                    this.getLogger().trace("The values for member {} " +
                                          "could not be aggregated " +
                                          "because not enough valid " +
                                          "member values could be " +
                                          "retrieved.",
                                          memberNumber);
                }

                if (aggregatedValues.size() > 0)
                {
                    this.climatology =
                            DefaultDataFactory.getInstance()
                                              .vectorOf( aggregatedValues.toArray( new Double[aggregatedValues.size()] ) );
                }
            }
            finally
            {
                if (results != null)
                {
                    results.close();
                }

                if (connection != null)
                {
                    Database.returnConnection( connection );
                }
            }
        }

        return this.climatology;
    }

    public MetricInputIterator( final ProjectConfig projectConfig, final Feature feature)
            throws SQLException, NoDataException,
            InvalidPropertiesFormatException
    {
        this.projectDetails = Projects.getProject( projectConfig );
        this.feature = feature;

        this.createLeftHandCache();

        if (this.leftHandMap == null || this.leftHandMap.size() == 0)
        {
            throw new NoDataException( "No data for the left hand side of " +
                                       " the evaluation could be loaded. " +
                                       "Please check your specifications." );
        }

        // TODO: This needs a better home
        // x2; 1 step for retrieval, 1 step for calculation
        ProgressMonitor.setSteps( Long.valueOf( this.getWindowCount() ) * 2 );
    }

    @Override
    public boolean hasNext()
    {
        boolean next = false;

        try
        {
            if (ConfigHelper.isForecast( this.getRight() ))
            {
                int nextWindowNumber = this.getWindowNumber() + 1;
                double beginning = this.getProjectDetails().getLead(nextWindowNumber) +
                                   this.getProjectDetails().getLeadOffset( this.getFeature() );
                double end = this.getProjectDetails().getLead(nextWindowNumber + 1) +
                             this.getProjectDetails().getLeadOffset( this.getFeature() );

                next = beginning < this.getProjectDetails().getLastLead( this.getFeature() ) &&
                       end >= this.getProjectDetails().getMinimumLeadHour() &&
                       end <= this.getProjectDetails().getLastLead( this.getFeature() );
            }
            else
            {
                next = this.getWindowNumber() == -1;
            }

            if (!next && this.getWindowNumber() < 0)
            {
                String message = "Due to the configuration of this project,";
                message += " there are no valid windows to evaluate. ";
                message += "The range of all lead times go from {} to ";
                message += "{}, and the size of the window is {} hours. ";
                message += "Based on the difference between the initialization ";
                message += "of the left and right data sets, there is a {} ";
                message += "hour offset. This puts an initial window out ";
                message += "range of the specifications.";

                this.getLogger().error(message,
                                       this.getProjectDetails().getMinimumLeadHour(),
                                       this.getProjectDetails().getLastLead( feature ),
                                       this.getProjectDetails().getWindowWidth(),
                                       this.getProjectDetails().getLeadOffset( this.getFeature() ));
            }
        }
        catch ( SQLException | InvalidPropertiesFormatException e )
        {
            this.getLogger().error( Strings.getStackTrace( e ));
        }
        catch ( NoDataException e )
        {
            this.getLogger().error("The last lead time for pairing could not be " +
                                   "determined; There is no data to pair and " +
                                   "iterate over.");
        }

        return next;
    }

    @Override
    public Future<MetricInput<?>> next()
    {
        Future<MetricInput<?>> nextInput = null;

        if (this.hasNext())
        {
            this.incrementWindowNumber();
            try
            {
                // TODO: Pass the leftHandMap instead of the function
                InputRetriever retriever = new InputRetriever( this.getProjectDetails(),
                                                               (String firstDate, String lastDate) -> {
                                                                   return Collections
                                                                           .getValuesInRange( this.leftHandMap, firstDate, lastDate );
                                                               });
                retriever.setFeature(feature);
                retriever.setClimatology( this.getClimatology() );
                retriever.setProgress( this.getWindowNumber() );
                retriever.setLeadOffset( this.getProjectDetails()
                                             .getLeadOffset( this.getFeature() ) );
                retriever.setOnRun( ProgressMonitor.onThreadStartHandler() );
                retriever.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

                nextInput = Database.submit(retriever);
            }
            catch ( NoDataException | SQLException | InvalidPropertiesFormatException e )
            {
                this.getLogger().error( Strings.getStackTrace( e ) );
                e.printStackTrace();
            }
        }

        return nextInput;
    }

    protected DataSourceConfig getSimulation()
    {
        DataSourceConfig simulation = null;

        if (!ConfigHelper.isForecast( this.getRight()))
        {
            simulation = this.getRight();
        }
        else if (!ConfigHelper.isForecast( this.getBaseline() ))
        {
            simulation = this.getBaseline();
        }

        return simulation;
    }

    protected DataSourceConfig getLeft()
    {
        return this.getProjectDetails().getLeft();
    }

    protected DataSourceConfig getRight()
    {
        return this.getProjectDetails().getRight();
    }

    protected DataSourceConfig getBaseline()
    {
        return this.getProjectDetails().getBaseline();
    }

    protected int getFirstLeadInWindow()
            throws InvalidPropertiesFormatException, NoDataException,
            SQLException
    {
        return ( this.getWindowNumber() * this.getProjectDetails().getWindowWidth()) +
               this.getProjectDetails().getLeadOffset( this.getFeature() );
    }

    protected String getEarliestObservationDate() throws SQLException
    {
        if (this.earliestObservationDate == null)
        {
            this.earliestObservationDate = this.projectDetails.getZeroDate( this.getLeft() );
        }
        return this.earliestObservationDate;
    }

    abstract int calculateWindowCount()
            throws SQLException,
            InvalidPropertiesFormatException,
            NoDataException;

    abstract void createLeftHandCache() throws SQLException, NoDataException;

    abstract Logger getLogger();
}
