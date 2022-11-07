package wres.io.project;

import java.time.Duration;
import java.time.MonthDay;
import java.util.Set;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.io.retrieval.DataAccessException;
import wres.datamodel.time.TimeWindowOuter;

/**
 * Provides an interface to the project declaration in combination with the ingested time-series data.
 * 
 * @author James Brown
 */

public interface Project
{
    /**
     * @return the project declaration
     */

    ProjectConfig getProjectConfig();

    /**
     * @return the measurement unit, which is either the declared unit or the analyzed unit, but possibly null
     * @throws DataAccessException if the measurement unit could not be determined
     * @throws IllegalArgumentException if the project identity is required and undefined
     */

    String getMeasurementUnit();

    /**
     * Returns the desired time scale. In order of availability, this is:
     * 
     * <ol>
     * <li>The desired time scale provided on construction;</li>
     * <li>The Least Common Scale (LCS) computed from the input data; or</li>
     * <li>The LCS computed from the <code>existingTimeScale</code> provided in the input declaration.</li>
     * </ol>
     * 
     * The LCS is the smallest common multiple of the time scales associated with every ingested dataset for a given 
     * project, variable and feature. The LCS is computed from all sides of a pairing (left, right and baseline) 
     * collectively. 
     * 
     * @return the desired time scale or null if unknown
     * @throws DataAccessException if the existing time scales could not be obtained
     */

    TimeScaleOuter getDesiredTimeScale();

    /**
     * Returns the set of {@link FeatureTuple} for the project. If none have been
     * created yet, then it is evaluated. If there is no specification within
     * the configuration, all locations that have been ingested are retrieved
     * @return a set of all feature tuples involved in the project
     * @throws DataAccessException if the features cannot be retrieved
     * @throws IllegalStateException if the features have not been set
     */

    Set<FeatureTuple> getFeatures();

    /**
     * Returns the set of {@link FeatureGroup} for the project.
     * @return A set of all feature groups involved in the project
     * @throws IllegalStateException if the features have not been set
     */

    Set<FeatureGroup> getFeatureGroups();

    /**
     * @param lrb the side of data for which the variable is required
     * @return the declared data source for the specified orientation
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    DataSourceConfig getDeclaredDataSource( LeftOrRightOrBaseline lrb );

    /**
     * @param lrb the side of data
     * @return whether there is lenient upscaling enforced for the specified side of data
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    boolean isUpscalingLenient( LeftOrRightOrBaseline lrb );

    /**
     * @param lrb the side of data for which the variable is required
     * @return the name of the variable for the specified side of data
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    String getVariableName( LeftOrRightOrBaseline lrb );

    /**
     * @param lrb the side of data for which the variable is required
     * @return the name of the declared variable for the specified side of data
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    String getDeclaredVariableName( LeftOrRightOrBaseline lrb );

    /**
     * @return the earliest analysis duration, defaults to {@link TimeWindowOuter#DURATION_MIN}.
     */

    Duration getEarliestAnalysisDuration();

    /**
     * @return the latest analysis duration, defaults to {@link TimeWindowOuter#DURATION_MAX}.
     */

    Duration getLatestAnalysisDuration();

    /**
     * @return The earliest possible day in a season or null
     */

    MonthDay getEarliestDayInSeason();

    /**
     * @return The latest possible day in a season or null
     */

    MonthDay getLatestDayInSeason();

    /**
     * @param dataSourceConfig the data source configuration
     * @return true if the data source uses gridded data, false otherwise
     */

    boolean usesGriddedData( DataSourceConfig dataSourceConfig );

    /**
     * Returns unique identifier for this project data
     * @return the project hash
     */

    String getHash();

    /**
     * @return whether or not baseline data is involved in the project
     */

    boolean hasBaseline();

    /**
     * @return whether or not there is a generated baseline
     */

    boolean hasGeneratedBaseline();

    /**
     * @return the project identity
     */

    long getId();

    /**
     * @return whether or not the project uses probability thresholds
     */

    boolean hasProbabilityThresholds();

    /**
     * Saves the project.
     * @return true if this call resulted in the project being saved, false otherwise
     * @throws DataAccessException if the save fails for any reason
     */

    boolean save();
}