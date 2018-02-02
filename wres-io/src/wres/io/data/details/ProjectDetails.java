package wres.io.data.details;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeScaleFunction;
import wres.config.generated.TimeWindowMode;
import wres.io.concurrency.ValueRetriever;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
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

    private final List<Integer> leftForecastIDs = new ArrayList<>(  );
    private final List<Integer> rightForecastIDs = new ArrayList<>(  );
    private final List<Integer> baselineForecastIDs = new ArrayList<>(  );

    private final Map<Integer, String> leftHashes = new ConcurrentHashMap<>(  );
    private final Map<Integer, String> rightHashes = new ConcurrentHashMap<>(  );
    private final Map<Integer, String> baselineHashes = new ConcurrentHashMap<>(  );
    private final Map<Feature, Integer> lastLeads = new ConcurrentHashMap<>(  );
    private final Map<Feature, Integer> leadOffsets = new ConcurrentSkipListMap<>( ConfigHelper.getFeatureComparator() );
    private final Map<Feature, String> zeroDates = new ConcurrentHashMap<>(  );
    private final Map<Feature, Integer> poolCounts = new ConcurrentHashMap<>(  );

    private final List<Integer> leftSources = new ArrayList<>(  );
    private final List<Integer> rightSources = new ArrayList<>(  );
    private final List<Integer> baselineSources = new ArrayList<>(  );

    private Set<FeatureDetails> features;

    private Integer leftVariableID = null;
    private Integer rightVariableID = null;
    private Integer baselineVariableID = null;

    private boolean performedInsert;

    private final int inputCode;

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
            this.features = Features.getAllDetails(this.projectConfig);
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

    public int getLeftTimeShift()
    {
        int shift = 0;

        if (this.getLeft().getTimeShift() != null)
        {
            shift = this.getLeft().getTimeShift().getWidth();
        }

        return shift;
    }

    public int getRightTimeShift()
    {
        int shift = 0;

        if (this.getRight().getTimeShift() != null)
        {
            shift = this.getRight().getTimeShift().getWidth();
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

    public Integer getLeadPeriod()
    {
        Integer period;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null &&
            this.projectConfig.getPair().getLeadTimesPoolingWindow().getPeriod() != null)
        {
            period = this.projectConfig.getPair()
                                       .getLeadTimesPoolingWindow()
                                       .getPeriod();
        }
        else if ( this.getScale() != null)
        {
            period = this.getScalingPeriod();
        }
        else
        {
            period = null;
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

    public String getLeadUnit()
    {
        String unit;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null)
        {
            unit = this.projectConfig.getPair().getLeadTimesPoolingWindow().getUnit().value();
        }
        else
        {
            unit = this.getScalingUnit();
        }

        return unit;
    }

    public Integer getLeadFrequency()
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
            frequency = this.getScalingFrequency();
        }

        return frequency;
    }

    public Integer getScalingPeriod()
    {
        Integer period;

        if ( this.getScale() != null)
        {
            period = this.getScale().getPeriod();
        }
        else
        {
            // TODO: write logic for determining the correct period from the existingTimeScale
            period = null;
        }

        return period;
    }

    public String getScalingUnit()
    {
        String unit;

        if ( this.getScale() != null)
        {
            unit = this.getScale().getUnit().value();
        }
        else
        {
            // TODO: Write logic for determining the correct unit from the existingTimeScale
            unit = null;
        }

        return unit;
    }

    public String getScalingFunction()
    {
        String function = null;

        if (this.projectConfig.getPair().getDesiredTimeScale() != null)
        {
            function = this.projectConfig.getPair()
                                         .getDesiredTimeScale()
                                         .getFunction()
                                         .value();
        }

        return function;
    }

    public int getScalingFrequency()
    {
        int frequency;

        if ( this.getScale() != null)
        {
            if ( this.getScale().getFrequency() != null )
            {
                frequency = this.getScale().getFrequency();
            }
            else
            {
                frequency = this.getScalingPeriod();
            }
        }
        else
        {
            // TODO: Write the logic for determining the frequency from the existing time scale
            frequency = 1;
        }

        frequency = TimeHelper.unitsToLeadUnits(
                this.getScalingUnit(),
                frequency
        ).intValue();

        return frequency;
    }

    public boolean shouldScale()
    {
        return this.getScale() != null &&
               !TimeScaleFunction.NONE
                       .value().equalsIgnoreCase(this.getScalingFunction());
    }

    public TimeScaleConfig getScale()
    {
        return this.projectConfig.getPair().getDesiredTimeScale();
    }

    // TODO: This is a piece to the puzzle of finding out how to magically
    // align the left and right data
    public Integer getCurrentRightHandPeriod()
    {
        Integer scale = 1;
        TimeScaleConfig timeScaleConfig = this.getLeft().getExistingTimeScale();

        if (timeScaleConfig == null)
        {
            // TODO: Somehow determine the scale (i.e. span between values) from the right hand data
        }
        else
        {
            scale = TimeHelper.unitsToLeadUnits(
                    timeScaleConfig.getUnit().value(),
                    timeScaleConfig.getPeriod()
            ).intValue();
        }

        return scale;
    }
    
    public PoolingWindowConfig getIssuePoolingWindow()
    {
        return this.projectConfig.getPair().getIssuedDatesPoolingWindow();
    }

    public TimeWindowMode getPoolingMode()
    {
        TimeWindowMode mode = TimeWindowMode.BACK_TO_BACK;

        // Can't use "this.getScalingFrequency because it uses this
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

    public String getDesiredMeasurementUnit()
    {
        return String.valueOf(this.projectConfig.getPair().getUnit());
    }

    public List<DestinationConfig> getPairDestinations()
    {
        return ConfigHelper.getPairDestinations( this.projectConfig );
    }

    public Integer getLeadOffset(Feature feature) throws SQLException, IOException
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
            throws IOException
    {
        FormattedStopwatch timer = new FormattedStopwatch();
        timer.start();

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
        Integer width = TimeHelper.unitsToLeadUnits( this.getLeadUnit(), this.getLeadPeriod() ).intValue();
/*
        if (this.getScale() != null)
        {
            width = TimeHelper.unitsToLeadUnits( this.getScalingUnit(), this.getScalingPeriod() ).intValue();
        }
        else
        {
            width = this.getCurrentRightHandPeriod();
        }
*/

        String script = "";

        script += "WITH forecast_positions AS" + NEWLINE;
        script += "(" + NEWLINE;
        script += "    SELECT TS.variableposition_id, feature_id" + NEWLINE;
        script += "    FROM wres.TimeSeries TS" + NEWLINE;
        script += "    INNER JOIN wres.VariableByFeature VBF" + NEWLINE;
        script += "        ON VBF.variableposition_id = TS.variableposition_id" + NEWLINE;
        script += "    WHERE VBF.variable_name = '" + this.getRightVariableName() + "'" + NEWLINE;

        Set<String> hucs = new HashSet<>(  );
        Set<String> lids = new HashSet<>(  );
        Set<String> rfcs = new HashSet<>(  );
        Set<Long> comids = new HashSet<>(  );
        Set<String> gageIds = new HashSet<>(  );

        for (Feature feature : this.getProjectConfig().getPair().getFeature())
        {
            if (feature.getComid() != null)
            {
                comids.add( feature.getComid() );
            }

            if (Strings.hasValue( feature.getLocationId()))
            {
                lids.add( feature.getLocationId().toUpperCase() );
            }

            if (Strings.hasValue( feature.getHuc() ))
            {
                hucs.add( feature.getHuc() + "%" );
            }

            if (Strings.hasValue( feature.getRfc() ))
            {
                rfcs.add( feature.getRfc().toUpperCase() );
            }

            if (Strings.hasValue( feature.getGageId() ))
            {
                gageIds.add(feature.getGageId());
            }
        }

        String lidStatement = "";
        String comidStatement = "";
        String hucStatement = "";
        String gageStatement = "";
        String rfcStatement = "";

        if (lids.size() > 0)
        {
            lidStatement += "        AND VBF.lid = ";
            lidStatement += wres.util.Collections.formAnyStatement( lids, "text" );
            lidStatement += NEWLINE;
        }

        if (comids.size() > 0)
        {
            comidStatement += "        AND VBF.comid = ";
            comidStatement += wres.util.Collections.formAnyStatement( comids, "int" );
            comidStatement += NEWLINE;
        }

        if (hucs.size() > 0)
        {
            hucStatement += "        AND VBF.huc LIKE ";
            hucStatement += wres.util.Collections.formAnyStatement( hucs, "text" );
            hucStatement += NEWLINE;
        }

        if (gageIds.size() > 0)
        {
            gageStatement += "        AND VBF.gage_id = ";
            gageStatement += wres.util.Collections.formAnyStatement( gageIds, "text" );
            gageStatement += NEWLINE;
        }

        if (rfcs.size() > 0)
        {
            rfcStatement += "        AND VBF.rfc = ";
            rfcStatement += wres.util.Collections.formAnyStatement( rfcs, "text" );
            rfcStatement += NEWLINE;
        }

        script += lidStatement;
        script += comidStatement;
        script += hucStatement;
        script += gageStatement;
        script += rfcStatement;

        script += "        AND EXISTS (" + NEWLINE;
        script += "            SELECT 1" + NEWLINE;
        script += "            FROM wres.ProjectSource PS" + NEWLINE;
        script += "            INNER JOIN wres.ForecastSource FS" + NEWLINE;
        script += "                ON FS.source_id = PS.source_id" + NEWLINE;
        script += "            WHERE PS.project_id = " + this.getId() + NEWLINE;
        script += "                AND PS.member = 'right'" + NEWLINE;
        script += "                AND FS.forecast_id = TS.timeseries_id" + NEWLINE;
        script += "         )" + NEWLINE;
        script += "    GROUP BY TS.variableposition_id, VBF.feature_id" + NEWLINE;
        script += ")," + NEWLINE;
        script += "observation_positions AS " + NEWLINE;
        script += "(" + NEWLINE;
        script += "    SELECT VBF.variableposition_id, feature_id" + NEWLINE;
        script += "    FROM wres.Observation O" + NEWLINE;
        script += "    INNER JOIN wres.VariableByFeature VBF" + NEWLINE;
        script += "        ON VBF.variableposition_id = O.variableposition_id" + NEWLINE;
        script += "    WHERE VBF.variable_name = '" + this.getLeftVariableName() + "'" + NEWLINE;

        script += lidStatement;
        script += comidStatement;
        script += hucStatement;
        script += gageStatement;
        script += rfcStatement;

        script += "        AND EXISTS (" + NEWLINE;
        script += "            SELECT 1" + NEWLINE;
        script += "            FROM wres.ProjectSource PS" + NEWLINE;
        script += "            WHERE PS.project_id = " + this.getId() + NEWLINE;
        script += "                AND PS.member = 'left'" + NEWLINE;
        script += "                AND PS.source_id = O.source_id" + NEWLINE;
        script += "         )" + NEWLINE;
        script += "    GROUP BY VBF.variableposition_id, feature_id" + NEWLINE;
        script += ")" + NEWLINE;
        script += "SELECT FP.variableposition_id AS forecast_position," + NEWLINE;
        script += "    O.variableposition_id AS observation_position," + NEWLINE;
        script += "    comid," + NEWLINE;
        script += "    gage_id," + NEWLINE;
        script += "    huc," + NEWLINE;
        script += "    lid" + NEWLINE;
        script += "FROM forecast_positions FP" + NEWLINE;
        script += "INNER JOIN observation_positions O" + NEWLINE;
        script += "    ON O.feature_id = FP.feature_id" + NEWLINE;
        script += "INNER JOIN wres.Feature F" + NEWLINE;
        script += "    ON O.feature_id = F.feature_id;";

        String beginning = "";

        // TODO: An invalid width is determined by the
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
            Integer offset = null;
            try
            {
                LOGGER.trace( "Loading the offset for '{}'", ConfigHelper.getFeatureDescription( feature ));
                offset = futureOffset.get( 500, TimeUnit.MILLISECONDS);
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
                             + "offsets. Trying again...", e);
            }
            catch ( TimeoutException e )
            {
                LOGGER.trace("It took took too long to get the offset for '{}'; "
                             + "moving on to another location while we wait "
                             + "for the output on this location",
                             ConfigHelper.getFeatureDescription( feature ));
                futureOffsets.put( key, futureOffset );
            }
        }

        timer.stop();
        LOGGER.debug( "It took {} to get the offsets for all locations.", timer.getFormattedDuration() );
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

                this.poolCounts.put( feature, Database.getValue( resultSet, "window_count" ));
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

    private void loadForecastIDs(String member) throws SQLException
    {

        Integer memberVariableID;
        List<Integer> forecastIDs;

        if (member.equals( ProjectDetails.LEFT_MEMBER ))
        {
            memberVariableID = this.getLeftVariableID();
            forecastIDs = this.leftForecastIDs;
        }
        else if (member.equals( ProjectDetails.RIGHT_MEMBER ))
        {
            memberVariableID = this.getRightVariableID();
            forecastIDs = this.rightForecastIDs;
        }
        else
        {
            memberVariableID = this.getBaselineVariableID();
            forecastIDs = this.baselineForecastIDs;
        }

        StringBuilder script = new StringBuilder(  );
        script.append( "SELECT TS.timeseries_id" ).append(NEWLINE);
        script.append( "FROM wres.ForecastSource FS").append(NEWLINE);
        script.append( "INNER JOIN wres.ProjectSource PS").append(NEWLINE);
        script.append( "    ON PS.source_id = FS.source_id").append(NEWLINE);
        script.append( "INNER JOIN wres.TimeSeries TS").append(NEWLINE);
        script.append( "    ON TS.timeseries_id = FS.forecast_id").append(NEWLINE);
        script.append( "INNER JOIN wres.VariablePosition VP").append(NEWLINE);
        script.append( "    ON VP.variableposition_id = TS.variableposition_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(member).append(NEWLINE);
        script.append("     AND PS.inactive_time IS NULL").append(NEWLINE);
        script.append("     AND VP.variable_id = ").append(memberVariableID).append(";");

        Database.populateCollection(forecastIDs,
                                    script.toString(),
                                    "timeseries_id");
    }

    private void loadSources(String member) throws SQLException
    {
        StringBuilder script = new StringBuilder(  );

        script.append( "SELECT PS.source_id, S.hash").append(NEWLINE);
        script.append( "FROM wres.ProjectSource PS").append(NEWLINE);
        script.append( "INNER JOIN wres.Source S").append(NEWLINE);
        script.append( "    ON S.source_id = PS.source_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(member).append(NEWLINE);
        script.append( "    AND PS.inactive_time IS NULL;").append(NEWLINE);

        Connection connection = null;
        ResultSet resultSet = null;

        Map<Integer, String> sourceHashes;
        List<Integer> sourceIDs;

        if (member.equals( ProjectDetails.LEFT_MEMBER ))
        {
            sourceHashes = this.leftHashes;
            sourceIDs = this.leftSources;
        }
        else if (member.equals( ProjectDetails.RIGHT_MEMBER ))
        {
            sourceHashes = this.rightHashes;
            sourceIDs = this.rightSources;
        }
        else
        {
            sourceHashes = this.baselineHashes;
            sourceIDs = this.baselineSources;
        }

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults( connection, script.toString( ) );

            while (resultSet.next())
            {
                sourceIDs.add( resultSet.getInt( "source_id" ) );
                sourceHashes.put(resultSet.getInt( "source_id" ),
                                    resultSet.getString( "hash" ));
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

    public Integer getLead(int windowNumber)
            throws InvalidPropertiesFormatException
    {
        return TimeHelper.unitsToLeadUnits( this.getLeadUnit(),
                                  1.0 * windowNumber +
                                  this.getLeadFrequency() * this.getLeadPeriod()).intValue();
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

    public int getWindowWidth() throws InvalidPropertiesFormatException
    {
        return TimeHelper.unitsToLeadUnits( this.getLeadUnit(), this.getLeadPeriod() ).intValue();
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

