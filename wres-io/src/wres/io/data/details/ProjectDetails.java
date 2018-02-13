package wres.io.data.details;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DurationUnit;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeScaleFunction;
import wres.config.generated.TimeWindowMode;
import wres.io.concurrency.DataSetRetriever;
import wres.io.concurrency.ValueRetriever;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.utilities.DataSet;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.io.utilities.ScriptGenerator;
import wres.util.FormattedStopwatch;
import wres.util.Strings;
import wres.util.TimeHelper;

/**
 * Wrapper object linking a project configuration and the data needed to form
 * database statements
 */
public class ProjectDetails extends CachedDetail<ProjectDetails, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectDetails.class );

    public static final String LEFT_MEMBER = "'left'";
    public static final String RIGHT_MEMBER = "'right'";
    public static final String BASELINE_MEMBER = "'baseline'";

    private Integer projectID = null;
    private final ProjectConfig projectConfig;

    private final Map<Feature, Integer> lastLeads = new ConcurrentHashMap<>(  );
    private final Map<Feature, Integer> leadOffsets = new ConcurrentSkipListMap<>( ConfigHelper.getFeatureComparator() );
    private final Map<Feature, String> zeroDates = new ConcurrentHashMap<>(  );
    private final Map<Feature, Integer> poolCounts = new ConcurrentHashMap<>(  );

    private Set<FeatureDetails> features;

    private Integer leftVariableID = null;
    private Integer rightVariableID = null;
    private Integer baselineVariableID = null;

    private boolean performedInsert;

    private final int inputCode;

    private long leftScale = -1;
    private long rightScale = -1;
    private long baselineScale = -1;
    private TimeScaleConfig desiredTimeScale;

    private Boolean calculateLeads = null;
    private Map<Feature, Boolean> calculateFeatureLeads = null;
    private Map<Feature, Integer[]> discreteLeads;

    private static final Object POOL_LOCK = new Object();

    public static Integer hash( final ProjectConfig projectConfig,
                                final List<String> leftHashesIngested,
                                final List<String> rightHashesIngested,
                                final List<String> baselineHashesIngested )
    {
        StringBuilder hashBuilder = new StringBuilder(  );

        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        hashBuilder.append(left.getType().value());

        for ( EnsembleCondition ensembleCondition : left.getEnsemble())
        {
            hashBuilder.append(ensembleCondition.getName());
            hashBuilder.append(ensembleCondition.getMemberId());
            hashBuilder.append(ensembleCondition.getQualifier());
        }

        // Sort for deterministic hash result for same list of ingested
        List<String> sortedLeftHashes =
                ProjectDetails.copyAndSort( leftHashesIngested );

        for ( String leftHash : sortedLeftHashes )
        {
            hashBuilder.append( leftHash );
        }

        hashBuilder.append(left.getVariable().getValue());
        hashBuilder.append(left.getVariable().getUnit());

        hashBuilder.append(right.getType().value());

        for ( EnsembleCondition ensembleCondition : right.getEnsemble())
        {
            hashBuilder.append(ensembleCondition.getName());
            hashBuilder.append(ensembleCondition.getMemberId());
            hashBuilder.append(ensembleCondition.getQualifier());
        }

        // Sort for deterministic hash result for same list of ingested
        List<String> sortedRightHashes =
                ProjectDetails.copyAndSort( rightHashesIngested );

        for ( String rightHash : sortedRightHashes )
        {
            hashBuilder.append( rightHash );
        }

        hashBuilder.append(right.getVariable().getValue());
        hashBuilder.append(right.getVariable().getUnit());

        if (baseline != null)
        {

            hashBuilder.append(baseline.getType().value());

            for ( EnsembleCondition ensembleCondition : baseline.getEnsemble())
            {
                hashBuilder.append(ensembleCondition.getName());
                hashBuilder.append(ensembleCondition.getMemberId());
                hashBuilder.append(ensembleCondition.getQualifier());
            }


            // Sort for deterministic hash result for same list of ingested
            List<String> sortedBaselineHashes =
                    ProjectDetails.copyAndSort( baselineHashesIngested );

            for ( String baselineHash : sortedBaselineHashes )
            {
                hashBuilder.append( baselineHash );
            }

            hashBuilder.append(baseline.getVariable().getValue());
            hashBuilder.append(baseline.getVariable().getUnit());
        }

        for ( Feature feature : projectConfig.getPair()
                                             .getFeature() )
        {
            hashBuilder.append( ConfigHelper.getFeatureDescription( feature ) );
        }

        return hashBuilder.toString().hashCode();
    }

    public ProjectDetails( ProjectConfig projectConfig,
                           Integer inputCode )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( inputCode );
        this.projectConfig = projectConfig;
        this.inputCode = inputCode;
    }

    @Override
    public Integer getKey()
    {
        return this.getInputCode();
    }

    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    public String getInputName(DataSourceConfig dataSourceConfig)
    {
        String name = null;

        if (dataSourceConfig.equals( this.getLeft() ))
        {
            name = ProjectDetails.LEFT_MEMBER;
        }
        else if (dataSourceConfig.equals( this.getRight() ))
        {
            name = ProjectDetails.RIGHT_MEMBER;
        }
        else if (dataSourceConfig.equals( this.getBaseline() ))
        {
            name = ProjectDetails.BASELINE_MEMBER;
        }

        return name;
    }

    public Set<FeatureDetails> getFeatures() throws SQLException
    {
        if (this.features == null)
        {
            if (this.getProjectConfig().getPair().getFeature() == null || this.projectConfig.getPair().getFeature().size() == 0)
            {
                this.features = new HashSet<>(  );

                ScriptBuilder script = new ScriptBuilder();

                script.addLine( "SELECT *" );
                script.addLine( "FROM wres.Feature F" );
                script.addLine( "WHERE EXISTS (" );

                if ( ConfigHelper.isForecast( this.getLeft() ) )
                {
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.TimeSeries TS" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariablePosition VP" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VP.variableposition_id = TS.variableposition_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ForecastSource FS" );
                    script.addTab( 2 )
                          .addLine( "ON FS.forecast_id = TS.timeseries_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = FS.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
                    script.addTab( 2 )
                          .addLine( "AND VP.x_position = F.feature_id" );
                }
                else
                {
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.Observation O" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariablePosition VP" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VP.variableposition_id = O.variableposition_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = O.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
                    script.addTab( 2 )
                          .addLine( "AND VP.x_position = F.feature_id" );
                }

                script.addTab().addLine( ")" );
                script.addTab().addLine( "AND EXISTS (" );

                if ( ConfigHelper.isForecast( this.getRight() ) )
                {
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.TimeSeries TS" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariablePosition VP" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VP.variableposition_id = TS.variableposition_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ForecastSource FS" );
                    script.addTab( 2 )
                          .addLine( "ON FS.forecast_id = TS.timeseries_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = FS.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
                    script.addTab( 2 )
                          .addLine( "AND VP.x_position = F.feature_id" );
                }
                else
                {
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.Observation O" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariablePosition VP" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VP.variableposition_id = O.variableposition_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = O.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
                    script.addTab( 2 )
                          .addLine( "AND VP.x_position = F.feature_id" );
                }

                script.addLine( ");" );

                Connection connection = null;
                ResultSet resultSet = null;

                try
                {
                    connection = Database.getConnection();
                    resultSet = Database.getResults( connection,
                                                     script.toString() );

                    while ( resultSet.next() )
                    {
                        this.features.add( new FeatureDetails( resultSet ) );
                    }
                }
                catch ( SQLException e )
                {
                    throw new SQLException(
                            "The features for this project could "
                            + "not be retrieved from the database.",
                            e );
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
            }
            else
            {
                this.features = Features.getAllDetails( this.getProjectConfig() );
            }
        }
        return this.features;
    }

    public DataSourceConfig getLeft()
    {
        return this.projectConfig.getInputs().getLeft();
    }

    public DataSourceConfig getRight()
    {
        return this.projectConfig.getInputs().getRight();
    }

    public DataSourceConfig getBaseline()
    {
        return this.projectConfig.getInputs().getBaseline();
    }

    public Integer getLeftVariableID() throws SQLException
    {
        if (this.leftVariableID == null)
        {
            this.leftVariableID = Variables.getVariableID(this.getLeftVariableName(),
                                                          this.getLeftVariableUnit());
        }

        return this.leftVariableID;
    }

    public String getLeftVariableName()
    {
        return this.getLeft().getVariable().getValue();
    }

    public String getLeftVariableUnit()
    {
        return this.getLeft().getVariable().getUnit();
    }

    public Integer getRightVariableID() throws SQLException
    {
        if (this.rightVariableID == null)
        {
            this.rightVariableID = Variables.getVariableID( this.getRightVariableName(),
                                                            this.getRightVariableUnit());
        }

        return this.rightVariableID;
    }

    public String getRightVariableName()
    {
        return this.getRight().getVariable().getValue();
    }

    public String getRightVariableUnit()
    {
        return this.getRight().getVariable().getUnit();
    }

    public Integer getBaselineVariableID() throws SQLException
    {
        if (this.hasBaseline() && this.baselineVariableID == null)
        {
            this.baselineVariableID = Variables.getVariableID( this.getBaselineVariableName(),
                                                               this.getBaselineVariableUnit() );
        }

        return this.baselineVariableID;
    }

    public String getBaselineVariableName()
    {
        String name = null;
        if (this.hasBaseline())
        {
            name = this.getBaseline().getVariable().getValue();
        }
        return name;
    }

    public String getBaselineVariableUnit()
    {
        String unit = null;
        if (this.hasBaseline())
        {
            unit = this.getBaseline().getVariable().getUnit();
        }
        return unit;
    }

    public long getLeftTimeShift()
    {
        long shift = 0;

        if (this.getLeft().getTimeShift() != null)
        {
            shift = TimeHelper.unitsToLeadUnits( this.getLeft().getTimeShift()
                                                     .getUnit()
                                                     .value(),
                                                 this.getLeft().getTimeShift().getWidth()
            );
        }

        return shift;
    }

    public long getRightTimeShift()
    {
        long shift = 0;

        if (this.getRight().getTimeShift() != null)
        {
            shift = TimeHelper.unitsToLeadUnits( this.getRight().getTimeShift()
                                                     .getUnit()
                                                     .value(),
                                                 this.getRight().getTimeShift().getWidth()
            );
        }

        return shift;
    }

    public Double getMaximumValue()
    {
        Double maximum = Double.MAX_VALUE;

        if (this.projectConfig.getPair().getValues() != null &&
            this.projectConfig.getPair().getValues().getMaximum() != null)
        {
            maximum = this.projectConfig.getPair().getValues().getMaximum();
        }

        return maximum;
    }

    public Double getMinimumValue()
    {
        Double minimum = -Double.MAX_VALUE;

        if (this.projectConfig.getPair().getValues() != null &&
            this.projectConfig.getPair().getValues().getMinimum() != null)
        {
            minimum = this.projectConfig.getPair().getValues().getMinimum();
        }

        return minimum;
    }

    public String getEarliestDate()
    {
        String earliestDate = null;

        if (this.projectConfig.getPair().getDates() != null &&
            this.projectConfig.getPair().getDates().getEarliest() != null)
        {
            earliestDate = this.projectConfig.getPair().getDates().getEarliest();
        }

        return earliestDate;
    }

    public String getLatestDate()
    {
        String latestDate = null;

        if (this.projectConfig.getPair().getDates() != null &&
                this.projectConfig.getPair().getDates().getLatest() != null)
        {
            latestDate = this.projectConfig.getPair().getDates().getLatest();
        }

        return latestDate;
    }

    public String getEarliestIssueDate()
    {
        String earliestDate = null;

        if (this.projectConfig.getPair().getIssuedDates() != null &&
                this.projectConfig.getPair().getIssuedDates().getEarliest() != null)
        {
            earliestDate = this.projectConfig.getPair().getIssuedDates().getEarliest();
        }

        return earliestDate;
    }

    public String getLatestIssueDate()
    {
        String latestDate = null;

        if (this.projectConfig.getPair().getIssuedDates() != null &&
            this.projectConfig.getPair().getIssuedDates().getLatest() != null)
        {
            latestDate = this.projectConfig.getPair().getIssuedDates().getLatest();
        }

        return latestDate;
    }

    public boolean specifiesSeason()
    {
        return this.projectConfig.getPair().getSeason() != null;
    }

    public MonthDay getEarliestDayInSeason()
    {
        MonthDay earliest = null;

        PairConfig.Season season = this.projectConfig.getPair().getSeason();

        if (season != null)
        {
            earliest = MonthDay.of(season.getEarliestMonth(), season.getEarliestDay());
        }

        return earliest;
    }

    public MonthDay getLatestDayInSeason()
    {
        MonthDay latest = null;

        PairConfig.Season season = this.projectConfig.getPair().getSeason();

        if (season != null)
        {
            latest = MonthDay.of(season.getLatestMonth(), season.getLatestDay());
        }

        return latest;
    }

    public Integer getLeadPeriod() throws NoDataException
    {
        Integer period;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null &&
            this.projectConfig.getPair().getLeadTimesPoolingWindow().getPeriod() != null)
        {
            period = this.projectConfig.getPair()
                                       .getLeadTimesPoolingWindow()
                                       .getPeriod();
        }
        else
        {
            period = this.getScale().getPeriod();
        }

        return period;
    }

    public int getIssuePoolingWindowPeriod()
    {
        // Indicates one basis per period
        int period = 0;

        if (this.getIssuePoolingWindow().getPeriod() != null)
        {
            period = this.getIssuePoolingWindow().getPeriod();
        }

        return period;
    }

    public String getLeadUnit() throws NoDataException
    {
        String unit;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null)
        {
            unit = this.projectConfig.getPair().getLeadTimesPoolingWindow().getUnit().value();
        }
        else
        {
            unit = this.getScale().getUnit().value();
        }

        return unit;
    }

    public Integer getLeadFrequency() throws NoDataException
    {
        Integer frequency;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null)
        {
            if (this.projectConfig.getPair().getLeadTimesPoolingWindow().getFrequency() != null)
            {
                frequency = this.projectConfig.getPair()
                                              .getLeadTimesPoolingWindow()
                                              .getFrequency();
            }
            else
            {
                frequency = this.projectConfig.getPair()
                                              .getLeadTimesPoolingWindow()
                                              .getPeriod();
            }
        }
        else
        {
            frequency = this.getScale().getFrequency();
            if (frequency == null)
            {
                frequency = this.getScale().getPeriod();
            }
        }

        return frequency;
    }

    private boolean shouldDynamicallyPoolByLeads()
    {
        return this.projectConfig.getPair().getDesiredTimeScale() == null &&
               this.projectConfig.getPair().getLeadTimesPoolingWindow() == null &&
                     (this.projectConfig.getInputs().getLeft().getExistingTimeScale() == null ||
                      this.projectConfig.getInputs().getRight().getExistingTimeScale() == null);
    }

    public boolean shouldScale() throws NoDataException
    {
        return this.getScale() != null &&
               !TimeScaleFunction.NONE
                       .value().equalsIgnoreCase(this.getScale().getFunction().value());
    }

    private TimeScaleConfig getCommonScale()
            throws NoDataException, ProjectConfigException
    {
        Long commonScale;

        // We're assuming we'll be scaling, so we'll need a default function
        // used to aggregate our values. For the time being, we're going to
        // roll with the average
        TimeScaleFunction scaleFunction = TimeScaleFunction.AVG;

        try
        {
            long left = this.getLeftScale();
            long right = this.getRightScale();

            if (left == right)
            {
                commonScale = left;

                // Since the scales are actually the same, we're not going to
                // do any aggregation at all
                scaleFunction = TimeScaleFunction.NONE;
            }
            // This logic will attempt to reconcile the two to find a possible
            // desired scale; i.e. if the left is in a scale of 4 hours and the
            // right in 3, the needed scale would be 12 hours.
            else if (Math.max(left, right) % Math.min(left, right) == 0)
            {
                commonScale = Math.max(left, right);
            }
            else
            {

                BigInteger bigLeft = BigInteger.valueOf( left );

                Integer greatestCommonFactor =
                        bigLeft.gcd( BigInteger.valueOf( right ) )
                               .intValue();

                commonScale =
                        left * right / greatestCommonFactor;
            }
        }
        catch ( NoDataException e )
        {
            throw new NoDataException( "The common scale between left and"
                                       + "right inputs could not be evaluated.",
                                       e );
        }
        catch ( SQLException e )
        {
            throw new NoDataException( "The database could not determine "
                                       + "the scale of the left and right hand data.",
                                       e );
        }

        return new TimeScaleConfig(
                scaleFunction,
                commonScale.intValue(),
                commonScale.intValue(),
                DurationUnit.HOURS,
                "Dynamic Scale" );
    }

    public TimeScaleConfig getScale() throws NoDataException
    {
        if (this.desiredTimeScale == null)
        {
            this.desiredTimeScale = this.projectConfig.getPair().getDesiredTimeScale();
        }

        if (this.desiredTimeScale == null)
        {
            try
            {
                this.desiredTimeScale = this.getCommonScale();
            }
            catch ( ProjectConfigException e )
            {
                throw new NoDataException(
                        "There was not enough information on hand to determine "
                        + "to determine the scope.",
                        e
                );
            }
        }

        return this.desiredTimeScale;
    }
    
    public PoolingWindowConfig getIssuePoolingWindow()
    {
        return this.projectConfig.getPair().getIssuedDatesPoolingWindow();
    }

    public TimeWindowMode getPoolingMode()
    {
        TimeWindowMode mode = TimeWindowMode.BACK_TO_BACK;

        if ( this.getIssuePoolingWindow() != null )
        {
            mode = TimeWindowMode.ROLLING;
        }

        return mode;
    }
    
    public String getIssuePoolingWindowUnit()
    {
        String unit = null;

        if ( this.projectConfig.getPair()
                               .getIssuedDatesPoolingWindow() != null )
        {
            unit = this.projectConfig.getPair()
                                     .getIssuedDatesPoolingWindow()
                                     .getUnit()
                                     .value();
        }

        return unit;
    }

    public int getIssuePoolingWindowFrequency()
    {
        // If there isn't a definition, we want to move a single unit at a time
        int frequency = 1;

        if (this.getIssuePoolingWindow().getFrequency() != null)
        {
            frequency = this.getIssuePoolingWindow().getFrequency();
        }
        else if ( this.getIssuePoolingWindowPeriod() > 0)
        {
            frequency = this.getIssuePoolingWindowPeriod();
        }

        return frequency;
    }

    private boolean shouldCalculateLeads() throws SQLException
    {
        if (this.calculateLeads == null)
        {
            this.calculateLeads = !this.shouldDynamicallyPoolByLeads();

            if (!this.calculateLeads)
            {
                ScriptBuilder script = new ScriptBuilder(  );

                script.addLine("WITH unique_leads AS" );
                script.addLine("(");
                script.addTab().addLine("SELECT FV.lead, lag(FV.lead) OVER ( ORDER BY FV.lead)");
                script.addTab().addLine("FROM wres.ForecastValue FV");
                script.addTab().addLine("WHERE FV.lead > 0");

                if (this.getMaximumLeadHour() != Integer.MAX_VALUE)
                {
                    script.addTab(2).addLine("AND FV.lead <= ", this.getMaximumLeadHour());
                }

                script.addTab(2).addLine("AND EXISTS (");
                script.addTab(3).addLine("SELECT 1");
                script.addTab(3).addLine("FROM wres.ForecastSource FS");
                script.addTab(3).addLine("INNER JOIN wres.ProjectSource PS");
                script.addTab(4).addLine("ON PS.source_id = FS.source_id");
                script.addTab(3).addLine("WHERE FS.forecast_id = FV.timeseries_id");
                script.addTab(4).addLine("AND PS.project_id = ", this.getId());
                script.addTab(4).addLine("AND PS.member = 'right'");
                script.addTab(2).addLine(")");
                script.addTab().addLine("GROUP BY FV.lead");
                script.addLine(")");
                script.addLine("SELECT MAX(row_number) = 1 AS is_regular");
                script.addLine("FROM (");
                script.addTab().addLine( "SELECT lead - lag, row_number() OVER (ORDER BY lead - lag)" );
                script.addTab().addLine( "FROM unique_leads");
                script.addTab().addLine( "WHERE lag IS NOT NULL");
                script.addTab().addLine( "GROUP BY lead - lag");
                script.addLine(") AS differences;");

                this.calculateLeads = script.retrieve( "is_regular" );
            }
        }

        return this.calculateLeads;
    }

    public String getDesiredMeasurementUnit()
    {
        return String.valueOf(this.projectConfig.getPair().getUnit());
    }

    public List<DestinationConfig> getPairDestinations()
    {
        return ConfigHelper.getPairDestinations( this.projectConfig );
    }

    public Pair<Integer, Integer> getLeadRange(final Feature feature, final int windowNumber)
            throws SQLException, IOException
    {
        Integer beginning;
        Integer end;

        if (this.shouldCalculateLeads())
        {
            Integer offset = this.getLeadOffset( feature );
            beginning = windowNumber * this.getLeadFrequency() + offset;
            end = beginning + this.getLeadPeriod();
        }
        else
        {
            if (this.discreteLeads == null)
            {
                populateDiscreteLeads();
            }

            beginning = this.discreteLeads.get( feature )[windowNumber];
            end = beginning;
        }

        return Pair.of( beginning, end );
    }

    public String getLeadQualifier(Feature feature, Integer windowNumber, String alias)
            throws IOException, SQLException
    {
        Pair<Integer, Integer> range = this.getLeadRange( feature, windowNumber );
        String qualifier = "";

        if (Strings.hasValue( alias ) && !alias.endsWith( "." ))
        {
            alias += ".";
        }
        else
        {
            alias = "";
        }

        if (Math.abs(range.getLeft() - range.getRight()) <= 1)
        {
            qualifier += alias;
            qualifier += "lead = ";
            qualifier += range.getRight();
        }
        else
        {
            qualifier += range.getLeft();
            qualifier += " < ";
            qualifier += alias;
            qualifier += "lead AND ";
            qualifier += alias;
            qualifier += "lead <= ";
            qualifier += range.getRight();
        }

        return qualifier;
    }

    private void populateDiscreteLeads() throws IOException
    {
        Connection connection = null;
        ResultSet resultSet = null;
        Map<FeatureDetails, Future<DataSet>> futureLeads = new LinkedHashMap<>(  );

        long width = Math.max(1, this.getMinimumLeadHour());

        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("SELECT FV.lead");
        script.addLine("FROM wres.ForecastValue FV");
        script.addLine("WHERE FV.lead >= ", width);

        if (this.getMaximumLeadHour() != Integer.MAX_VALUE)
        {
            script.addTab().addLine("AND FV.lead <= ", this.getMaximumLeadHour());
        }

        if (this.getMaximumValue() != Double.MAX_VALUE)
        {
            script.addTab().addLine("AND FV.forecasted_value <= ", this.getMaximumValue());
        }

        if (this.getMinimumValue() != -Double.MAX_VALUE)
        {
            script.addTab().addLine("AND FV.forecasted_value >= ", this.getMinimumValue());
        }

        script.addTab().addLine("AND EXISTS (");
        script.addTab( 2 ).addLine( "SELECT 1");
        script.addTab( 2 ).addLine( "FROM wres.TimeSeries TS");
        script.addTab( 2 ).addLine( "INNER JOIN wres.ForecastSource FS");
        script.addTab(  3  ).addLine(  "ON FS.forecast_id = TS.timeseries_id");
        script.addTab( 2 ).addLine( "INNER JOIN wres.ProjectSource PS");
        script.addTab(  3  ).addLine(  "ON PS.source_id = FS.source_id");
        script.addTab( 2 ).addLine( "WHERE TS.timeseries_id = FV.timeseries_id");
        script.addTab(  3  ).addLine(  "AND PS.project_id = ", this.getId());
        script.addTab(  3  ).addLine(  "AND PS.member = 'right'");

        if (Strings.hasValue( this.getEarliestIssueDate() ))
        {
            script.addTab(3).addLine("AND TS.initialization_date >= '", this.getEarliestIssueDate(), "'");
        }

        if (Strings.hasValue( this.getLatestIssueDate() ))
        {
            script.addTab(3).addLine("AND TS.initialization_date <= '", this.getLatestIssueDate(), "'");
        }

        if (Strings.hasValue(this.getEarliestDate()))
        {
            script.addTab(3).addLine("AND TS.initialization_date + INTERVAL '1 HOUR' * FV.lead' >= '", this.getEarliestDate(), "'");
        }

        if (Strings.hasValue( this.getLatestDate() ))
        {
            script.addTab(3).addLine("AND TS.initialization_date + INTERVAL '1 HOUR' * FV.lead <= '", this.getLatestDate(), "'");
        }

        String beginning = script.toString();

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults( connection,
                                             ScriptGenerator.formVariablePositionLoadScript( this, false ) );

            while (resultSet.next())
            {
                script = new ScriptBuilder( beginning );

                script.addTab(  3  ).addLine(  "AND TS.variableposition_id = ", resultSet.getInt( "forecast_position" ));
                script.addTab().addLine(")");
                script.addLine("GROUP BY FV.lead");
                script.addLine("ORDER BY FV.lead;");

                futureLeads.put( new FeatureDetails( resultSet ),
                                 Database.submit( new DataSetRetriever( script.toString() ) ) );
            }
        }
        catch (SQLException e)
        {
            throw new IOException( "Tasks used to determine discrete lead hours "
                                   + "could not be created.", e );
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch (SQLException e)
                {
                    LOGGER.debug("A database result set could not be closed.", e);
                }
            }

            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        while (!futureLeads.isEmpty())
        {
            FeatureDetails key = (FeatureDetails)futureLeads.keySet().toArray()[0];
            Feature feature = key.toFeature();
            Future<DataSet> futureDataSet = futureLeads.remove( key );
            List<Integer> leadList = new ArrayList<>();
            DataSet dataSet;
            try
            {
                // If we're on the last/only task, there's no reason to try and
                // cycle through it
                if (futureLeads.isEmpty())
                {
                    dataSet = futureDataSet.get();
                }
                else
                {
                    dataSet = futureDataSet.get( 500, TimeUnit.MILLISECONDS );
                }

                if (dataSet == null)
                {
                    continue;
                }

                while (dataSet.next())
                {
                    leadList.add(dataSet.getInt( "lead" ));
                }

                if (this.discreteLeads == null)
                {
                    this.discreteLeads = new TreeMap<>(ConfigHelper.getFeatureComparator());
                }

                this.discreteLeads.put( feature,
                                        leadList.toArray( new Integer[leadList.size()] ) );
            }
            catch ( InterruptedException e )
            {
                throw new IOException( "Population of discrete leads has been interrupted.", e );
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "An error occurred while populating discrete leads", e );
            }
            catch ( TimeoutException e )
            {
                LOGGER.trace("It took too long to get the set of discrete leads "
                             + "for '{}'; moving on to another location while "
                             + "we wait for the output for this location.",
                             ConfigHelper.getFeatureDescription( feature ));
                futureLeads.put( key, futureDataSet );
            }

        }
    }

    public Integer getLeadOffset(Feature feature)
            throws IOException, SQLException
    {
        if (ConfigHelper.isSimulation( this.getRight() ))
        {
            return 0;
        }

        if (this.leadOffsets.isEmpty())
        {
            this.populateLeadOffsets();
        }

        return this.leadOffsets.get( feature );
    }

    private void populateLeadOffsets()
            throws IOException, SQLException
    {
        FormattedStopwatch timer = null;

        if (LOGGER.isDebugEnabled())
        {
            timer = new FormattedStopwatch();
            timer.start();
        }

        // TODO: Implement dynamic scaling to determine the offset
        // This will end up grabbing the offset based off of the lead pool or
        // the scale, although we only ever need the offset in order to match
        // the scale. If there's not a need to scale, theoretically, we wouldn't
        // need to mess with a width. All seems to work out with the below
        // method based on the scale, but it starts to fall apart when there
        // isn't an "existing scale" tag; it defaults to an hour but won't
        // match if that's not the current scale.  For reference, see scenarios
        // 403 and 405. There needs to be a function that will find the least
        // common multiple of either the defined existing scales or the data in
        // the database.
        long width = TimeHelper.unitsToLeadUnits( this.getScale().getUnit().value(),
                                                     this.getScale().getPeriod());

        String script = ScriptGenerator.formVariablePositionLoadScript( this, true );

        String beginning = "";

        beginning += "SELECT (FV.lead - " + width + ")::integer AS offset" + NEWLINE;
        beginning += "FROM (" + NEWLINE;
        beginning += "    SELECT TS.timeseries_id, TS.initialization_date" + NEWLINE;
        beginning += "    FROM wres.TimeSeries TS" + NEWLINE;
        beginning += "    WHERE TS.variableposition_id = ";

        String middle = "";

        if (Strings.hasValue( this.getEarliestIssueDate() ))
        {
            middle += "        AND TS.initialization_date >= '" + this.getEarliestIssueDate() + "'" + NEWLINE;
        }

        if (Strings.hasValue( this.getLatestIssueDate() ))
        {
            middle += "        AND TS.initialization_date <= '" + this.getLatestIssueDate() + "'" + NEWLINE;
        }

        middle += "        AND EXISTS (" + NEWLINE;
        middle += "            SELECT 1" + NEWLINE;
        middle += "            FROM wres.ProjectSource PS" + NEWLINE;
        middle += "            INNER JOIN wres.ForecastSource FS" + NEWLINE;
        middle += "                ON FS.source_id = PS.source_id" + NEWLINE;
        middle += "            WHERE PS.project_id = " + this.getId() + NEWLINE;
        middle += "                AND PS.member = 'right'" + NEWLINE;
        middle += "                AND FS.forecast_id = TS.timeseries_id" + NEWLINE;
        middle += "        )" + NEWLINE;
        middle += "    ) AS TS" + NEWLINE;
        middle += "INNER JOIN wres.ForecastValue FV" + NEWLINE;
        middle += "    ON FV.timeseries_id = TS.timeseries_id" + NEWLINE;
        middle += "WHERE " + NEWLINE;

        boolean clauseAdded = false;

        if (width > 1)
        {
            middle += "FV.lead - " + width + " >= 0" + NEWLINE;
            clauseAdded = true;
        }

        if ( this.getMinimumLeadHour() != Integer.MIN_VALUE)
        {
            if (clauseAdded)
            {
                middle += "    AND ";
            }

            middle += "FV.lead >= " + this.getMinimumLeadHour() + NEWLINE;
            clauseAdded = true;
        }


        if ( this.getMaximumLeadHour() != Integer.MAX_VALUE )
        {
            if (clauseAdded)
            {
                middle += "    AND ";
            }

            middle += "FV.lead <= " + this.getMaximumLeadHour( ) + NEWLINE;
            clauseAdded = true;
        }

        if (clauseAdded)
        {
            middle += "    AND ";
        }

        middle += "EXISTS (" + NEWLINE;
        middle += "    SELECT 1" + NEWLINE;
        middle += "    FROM wres.ProjectSource PS" + NEWLINE;
        middle += "    INNER JOIN wres.observation o" + NEWLINE;
        middle += "        ON PS.source_id = O.source_id" + NEWLINE;
        middle += "    WHERE PS.project_id = " + this.getId() + NEWLINE;
        middle += "        AND PS.member = 'left'" + NEWLINE;
        middle += "        AND O.variableposition_id = ";

        String end = "";

        if (Strings.hasValue( this.getEarliestDate() ))
        {
            end += "        AND O.observation_time >= '" + this.getEarliestDate() + "'" + NEWLINE;
        }

        if (Strings.hasValue( this.getLatestDate() ))
        {
            end += "        AND O.observation_time <= '" + this.getLatestDate() + "'" + NEWLINE;
        }

        end += "        AND O.observation_time ";

        if (this.getLeft().getTimeShift() != null)
        {
            end += "+ '";
            end += this.getLeft().getTimeShift().getWidth();
            end += " ";
            end += this.getLeft().getTimeShift().getUnit().value();
            end += "' ";
        }

        end += "= TS.initialization_date + INTERVAL '1 HOUR' * (FV.lead + " + width + ")";

        if (this.getRight().getTimeShift() != null)
        {
            end += " + '";
            end += this.getRight().getTimeShift().getWidth();
            end += " ";
            end += this.getRight().getTimeShift().getUnit().value();
            end += "'";
        }

        end += NEWLINE;

        end += "    )" + NEWLINE;
        end += "ORDER BY FV.lead" + NEWLINE;
        end += "LIMIT 1;";

        Connection connection = null;
        ResultSet resultSet = null;
        Map<FeatureDetails.FeatureKey, Future<Integer>> futureOffsets = new LinkedHashMap<>(  );

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults( connection, script );

            LOGGER.trace("Variable position metadata loaded...");

            while (resultSet.next())
            {
                script = beginning +
                         Database.getValue( resultSet, "forecast_position" ) +
                         middle +
                         Database.getValue(resultSet, "observation_position") +
                         end;

                FeatureDetails.FeatureKey key = new FeatureDetails.FeatureKey(
                        Database.getValue(resultSet, "comid"),
                        Database.getValue(resultSet, "lid"),
                        Database.getValue( resultSet,"gage_id"),
                        Database.getValue(resultSet,"huc" )
                );

                futureOffsets.put(
                        key,
                        Database.submit(
                                new ValueRetriever<>(  script, "offset" )
                        )
                );

                LOGGER.trace( "A task has been created to find the offset for {}.", key );
            }
        }
        catch ( SQLException e )
        {
            throw new IOException("Tasks used to evaluate lead hour offsets "
                                  + "could not be created.", e);
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch ( SQLException e )
                {
                    LOGGER.debug("A database result set could not be closed.", e);
                }
            }

            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        while (!futureOffsets.isEmpty())
        {
            FeatureDetails.FeatureKey key = ( FeatureDetails.FeatureKey)futureOffsets.keySet().toArray()[0];
            Future<Integer> futureOffset = futureOffsets.remove( key );
            Feature feature = new FeatureDetails( key ).toFeature();
            Integer offset;
            try
            {
                LOGGER.trace( "Loading the offset for '{}'", ConfigHelper.getFeatureDescription( feature ));

                if (futureOffsets.isEmpty())
                {
                    offset = futureOffset.get();
                }
                else
                {
                    offset = futureOffset.get( 500, TimeUnit.MILLISECONDS );
                }

                if (offset == null)
                {
                    continue;
                }
                LOGGER.trace("The offset was: {}", offset);

                this.leadOffsets.put(
                        feature,
                        offset
                );
                LOGGER.trace( "An offset for {} was loaded!", ConfigHelper.getFeatureDescription( feature ) );
            }
            catch ( InterruptedException e )
            {
                throw new IOException( "Population of lead offsets has been interrupted.", e );
            }
            catch ( ExecutionException e )
            {
                throw new IOException("An error occured while populating the future"
                             + "offsets.", e);
            }
            catch ( TimeoutException e )
            {
                LOGGER.trace("It took too long to get the offset for '{}'; "
                             + "moving on to another location while we wait "
                             + "for the output on this location",
                             ConfigHelper.getFeatureDescription( feature ));
                futureOffsets.put( key, futureOffset );
            }
        }

        if (LOGGER.isDebugEnabled())
        {
            timer.stop();
            LOGGER.debug( "It took {} to get the offsets for all locations.",
                          timer.getFormattedDuration() );
        }
    }

    public Integer getPoolCount( Feature feature) throws SQLException
    {
        if ( getPoolingMode() != TimeWindowMode.ROLLING)
        {
            return -1;
        }

        synchronized ( POOL_LOCK )
        {
            if (!this.poolCounts.containsKey( feature ))
            {
                this.addPoolingFeature( feature );
            }
        }

        return this.poolCounts.get( feature );
    }

    private void addPoolingFeature( Feature feature) throws SQLException
    {
        String rollingScript = ScriptGenerator.formInitialRollingDataScript( this, feature );

        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults( connection, rollingScript );

            if (resultSet.isBeforeFirst())
            {
                resultSet.next();

                Integer windowCount = Database.getValue( resultSet, "window_count" );

                if (windowCount != null)
                {
                    this.poolCounts.put( feature,
                                         Database.getValue( resultSet,
                                                            "window_count" ) );
                }
                else
                {
                    throw new SQLException( "There was no intersection between "
                                            + "observation and forecast data for '"
                                            + ConfigHelper.getFeatureDescription( feature )
                                            + "'." );
                }
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
                Database.returnHighPriorityConnection( connection );
            }
        }
    }

    public String getProjectName()
    {
        return this.projectConfig.getName();
    }

    /**
     * Returns unique identifier for this project's config+data
     * @return The unique ID
     */
    private Integer getInputCode()
    {
        return this.inputCode;
    }

    public long getLeftScale() throws NoDataException, SQLException
    {
        if (this.leftScale == -1)
        {
            if (this.getLeft().getExistingTimeScale() == null)
            {
                this.leftScale = this.getScale( this.getLeft() );
            }
            else
            {
                this.leftScale =
                        TimeHelper.unitsToLeadUnits( this.getLeft()
                                                         .getExistingTimeScale()
                                                         .getUnit()
                                                         .value(),
                                                     this.getLeft()
                                                         .getExistingTimeScale()
                                                         .getPeriod() );
            }
        }

        return leftScale;
    }

    public long getRightScale() throws NoDataException, SQLException
    {
        if (this.rightScale == -1)
        {
            if (this.getRight().getExistingTimeScale() == null)
            {
                this.rightScale = this.getScale( this.getRight() );
            }
            else
            {
                this.rightScale =
                        TimeHelper.unitsToLeadUnits(
                                this.getRight().getExistingTimeScale().getUnit().value(),
                                this.getRight().getExistingTimeScale().getPeriod()
                        );
            }
        }

        return this.rightScale;
    }

    public long getBaselineScale() throws NoDataException, SQLException
    {
        if ( this.getBaseline() != null && this.baselineScale == -1)
        {
            if (this.getBaseline().getExistingTimeScale() == null)
            {
                this.baselineScale = this.getScale( this.getBaseline() );
            }
            else
            {
                this.baselineScale = TimeHelper.unitsToLeadUnits( this.getBaseline().getExistingTimeScale().getUnit().value(),
                                                                  this.getBaseline().getExistingTimeScale().getPeriod() );
            }
        }

        return this.baselineScale;
    }

    private int getScale(DataSourceConfig dataSourceConfig)
            throws NoDataException, SQLException
    {
        Integer leadScale;

        if (ConfigHelper.isForecast( dataSourceConfig ))
        {
            leadScale = this.getForecastScale( dataSourceConfig );
        }
        else
        {
            leadScale = this.getObservationScale( dataSourceConfig );
        }

        if (leadScale == null || leadScale < 1)
        {
            throw new NoDataException("The scale for the " +
                                      this.getInputName( dataSourceConfig ) +
                                      " either could not be determined or was "
                                      + "invalid. (lead = " +
                                      leadScale +
                                      ")");
        }

        return leadScale;
    }

    private Integer getForecastScale(DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("WITH differences AS");
        script.addLine("(");
        script.addLine("    SELECT lead - lag(lead) OVER (ORDER BY timeseries_id, lead) AS difference");
        script.addLine("    FROM wres.ForecastValue FV");
        script.addLine("    WHERE lead > 0");
        script.addLine("        AND EXISTS (");
        script.addLine("            SELECT 1");
        script.addLine("            FROM wres.ProjectSource PS");
        script.addLine("            INNER JOIN wres.ForecastSource FS");
        script.addLine("                ON FS.source_id = PS.source_id");
        script.addLine("            INNER JOIN wres.TimeSeries TS");
        script.addLine("                ON TS.timeseries_id = FS.forecast_id");
        script.addLine("            WHERE PS.project_id = ", this.getId());
        script.addLine("                AND PS.member = ", this.getInputName( dataSourceConfig ));
        script.addLine("                AND TS.timeseries_id = FV.timeseries_id");
        script.addLine("        )");
        script.addLine(")");
        script.addLine("SELECT MIN(difference)::integer AS scale");
        script.addLine("FROM differences");
        script.addLine("WHERE difference IS NOT NULL");
        script.addLine("    AND difference >= 0");

        return script.retrieve( "scale" );
    }

    private int getObservationScale(DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("WITH differences AS");
        script.addLine("(");
        script.addLine("    SELECT AGE(observation_time, (LAG(observation_time) OVER (ORDER BY observation_time)))");
        script.addLine("    FROM wres.Observation O");
        script.addLine("    WHERE EXISTS (");
        script.addLine("        SELECT 1");
        script.addLine("        FROM wres.ProjectSource PS");
        script.addLine("        WHERE PS.project_id = ", this.getId());
        script.addLine("            AND PS.member = ", this.getInputName( dataSourceConfig ) );
        script.addLine("            AND PS.source_id = O.source_id");
        script.addLine("    )");
        script.addLine("    GROUP BY observation_time");
        script.addLine(")");

        // TODO: When we change the scale of the lead column, we need to change this as well
        // We want the number of hours in the scale. It was previously "Extract ( hours ... ",
        // but, for 1 day, was returning 0 hours (considering there were 0 hours into the day
        // of the interval
        script.addLine("SELECT ( EXTRACT( epoch FROM MIN(age))/3600 )::integer AS scale");
        script.addLine("FROM differences");
        script.addLine("WHERE age IS NOT NULL");
        script.addLine("GROUP BY age;");

        return script.retrieve( "scale" );
    }

    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    @Override
    public Integer getId() {
        return this.projectID;
    }

    @Override
    protected String getIDName() {
        return "project_id";
    }

    @Override
    public void setID(Integer id) {
        this.projectID = id;
    }

    @Override
    protected String getInsertSelectStatement() {
        String script = "WITH new_project AS" + NEWLINE +
                        "(" + NEWLINE +
                        "     INSERT INTO wres.project (project_name, input_code)"
                        + NEWLINE +
                        "     SELECT '" + this.getProjectName() + "', " + this.getInputCode() + NEWLINE;

        script +=
                        "     WHERE NOT EXISTS (" + NEWLINE +
                        "         SELECT 1" + NEWLINE +
                        "         FROM wres.Project P" + NEWLINE +
                        "         WHERE P.input_code = " + this.getInputCode() + NEWLINE;

        script +=
                        "     )" + NEWLINE +
                        "     RETURNING project_id" + NEWLINE +
                        ")" + NEWLINE +
                        "SELECT project_id, TRUE as wasInserted" + NEWLINE +
                        "FROM new_project" + NEWLINE +
                        NEWLINE +
                        "UNION" + NEWLINE +
                        NEWLINE +
                        "SELECT project_id, FALSE as wasInserted" + NEWLINE +
                        "FROM wres.Project P" + NEWLINE +
                        "WHERE P.input_code = " + this.getInputCode() + ";";

        return script;
    }


    @Override
    public void save() throws SQLException
    {
        String[] tablesToLock = { "wres.project" };
        Pair<Integer,Boolean> databaseResult
                = Database.getResult( this.getInsertSelectStatement(),
                                      this.getIDName(),
                                      tablesToLock );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Did I create Project ID {}? {}",
                          databaseResult.getLeft(),
                          databaseResult.getRight() );
        }

        this.setID( databaseResult.getLeft() );
        this.performedInsert = databaseResult.getRight();
    }

    public boolean performedInsert()
    {
        return this.performedInsert;
    }

    public Integer getLastLead(Feature feature) throws SQLException
    {
        boolean leadIsMissing = !this.lastLeads.containsKey( feature );

        if (leadIsMissing)
        {
            String script = "";

            if ( ConfigHelper.isForecast( this.getRight() ) )
            {
                script += "SELECT MAX(FV.lead) AS last_lead" + NEWLINE;
                script += "FROM wres.TimeSeries TS" + NEWLINE;
                script += "INNER JOIN wres.ForecastValue FV" + NEWLINE;
                script += "    ON TS.timeseries_id = FV.timeseries_id" + NEWLINE;
                script += "WHERE " +
                          ConfigHelper.getVariablePositionClause( feature,
                                                                  this.getRightVariableID(),
                                                                  "TS" ) +
                          NEWLINE;

                if ( this.getMaximumLeadHour() != Integer.MAX_VALUE )
                {
                    script += "    AND FV.lead <= "
                              + this.getMaximumLeadHour( )
                              + NEWLINE;
                }

                if ( this.getMinimumLeadHour() != Integer.MIN_VALUE )
                {
                    script += "    AND FV.lead >= "
                              + this.getMinimumLeadHour( )
                              + NEWLINE;
                }

                if ( Strings.hasValue( this.getEarliestIssueDate()))
                {
                    script += "    AND TS.initialization_date >= '" + this.getEarliestIssueDate() + "'" + NEWLINE;
                }

                if (Strings.hasValue( this.getLatestIssueDate()))
                {
                    script += "    AND TS.initialization_date <= '" + this.getLatestIssueDate() + "'" + NEWLINE;
                }

                if ( Strings.hasValue( this.getEarliestDate() ))
                {
                    script += "    AND TS.initialization_date + INTERVAL '1 HOUR' * FV.lead >= '" + this.getEarliestDate() + "'" + NEWLINE;
                }

                if (Strings.hasValue( this.getLatestDate() ))
                {
                    script += "    AND TS.initialization_date + INTERVAL '1 HOUR' * FV.lead <= '" + this.getLatestDate() + "'" + NEWLINE;
                }

                script += "    AND EXISTS (" + NEWLINE;
                script += "        SELECT 1" + NEWLINE;
                script += "        FROM wres.ProjectSource PS" + NEWLINE;
                script += "        INNER JOIN wres.ForecastSource FS" + NEWLINE;
                script += "            ON FS.source_id = PS.source_id" + NEWLINE;
                script += "        WHERE PS.project_id = " + this.getId() + NEWLINE;
                script += "            AND PS.inactive_time IS NULL" + NEWLINE;
                script += "            AND FS.forecast_id = TS.timeseries_id" + NEWLINE;
                script += "    )";
            }
            else
            {
                script += "SELECT COUNT(*)::int AS last_lead" + NEWLINE;
                script += "FROM wres.Observation O" + NEWLINE;
                script += "INNER JOIN wres.ProjectSource PS" + NEWLINE;
                script += "     ON PS.source_id = O.source_id" + NEWLINE;
                script += "WHERE PS.project_id = " + this.getId() + NEWLINE;
                script += "     AND " +
                          ConfigHelper.getVariablePositionClause(
                                  feature,
                                  this.getRightVariableID(),
                                  "O" );
                script += NEWLINE;
            }
            script += ";";


            this.lastLeads.put(feature, Database.getResult( script, "last_lead" ));
        }

        return this.lastLeads.get(feature);
    }


    /**
     * Get the minimum lead hours from a project config or a default.
     * @return the minimum value specified or a default of Integer.MIN_VALUE
     */
    public Integer getMinimumLeadHour()
    {
        int result = Integer.MIN_VALUE;

        if ( this.getProjectConfig().getPair() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours()
                    .getMinimum() != null )
        {
            result = this.getProjectConfig().getPair()
                         .getLeadHours()
                         .getMinimum();
        }

        return result;
    }

    /**
     * Get the maximum lead hours from a project config or a default.
     * @return the maximum value specified or a default of Integer.MAX_VALUE
     */
    private int getMaximumLeadHour()
    {
        int result = Integer.MAX_VALUE;

        if ( this.getProjectConfig().getPair() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours()
                    .getMaximum() != null )
        {
            result = this.getProjectConfig().getPair()
                         .getLeadHours()
                         .getMaximum();
        }

        return result;
    }

    public long getWindowWidth() throws NoDataException
    {
        return TimeHelper.unitsToLeadUnits( this.getLeadUnit(), this.getLeadPeriod() );
    }

    public String getZeroDate(DataSourceConfig sourceConfig, Feature feature) throws SQLException
    {
        synchronized ( this.zeroDates )
        {
            if (!this.zeroDates.containsKey( feature ))
            {
                String script =
                        ScriptGenerator.generateZeroDateScript( this,
                                                                sourceConfig,
                                                                feature );
                this.zeroDates.put(feature, "'" + Database.getResult( script, "zero_date" ) + "'");
            }
        }

        return this.zeroDates.get(feature);
    }

    public boolean usesProbabilityThresholds()
    {
        return ConfigHelper.usesProbabilityThresholds( this.projectConfig );
    }

    @Override
    public int compareTo(ProjectDetails other)
    {
        return this.getInputCode().compareTo( other.getInputCode() );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null &&
               obj instanceof ProjectDetails &&
               this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return this.getInputCode();
    }

    private static List<String> copyAndSort( List<String> someList )
    {
        List<String> result = new ArrayList<>( someList.size() );
        result.addAll( someList );
        result.sort( Comparator.naturalOrder() );
        return Collections.unmodifiableList( result );
    }

}

