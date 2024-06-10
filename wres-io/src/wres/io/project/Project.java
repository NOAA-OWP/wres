package wres.io.project;

import java.time.Duration;
import java.time.MonthDay;
import java.util.Set;
import java.util.SortedSet;

import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Variable;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.io.retrieving.DataAccessException;
import wres.datamodel.time.TimeWindowOuter;

/**
 * <p>Wraps an {@link EvaluationDeclaration} and updates it to reflect the information ingested from time-series data
 * sources and, where applicable, threshold data sources. Specifically, {@link #getDeclaration()} provides an interface
 * to the augmented declaration. Also includes several helpers to assist with the use of a project declaration through
 * time-series data retrieval.
 *
 * <p>Implementation notes:
 *
 * <p>Concrete implementations decide how to access information about the time-series data, depending on how/where the
 * time-series data is stored (e.g., in a database).
 *
 * @author James Brown
 */

public interface Project
{
    /**
     * @return the project declaration
     */

    EvaluationDeclaration getDeclaration();

    /**
     * @return the measurement unit, which is either the declared unit or the analyzed unit, but possibly null
     * @throws DataAccessException if the measurement unit could not be determined
     * @throws IllegalArgumentException if the project identity is required and undefined
     */

    String getMeasurementUnit();

    /**
     * Returns the desired timescale. In order of availability, this is:
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
     * @return the desired timescale or null if unknown
     * @throws DataAccessException if the existing time scales could not be obtained
     */

    TimeScaleOuter getDesiredTimeScale();

    /**
     * Returns the set of {@link FeatureTuple} that appear in all contexts, whether as singletons or part of multi-
     * feature groups.
     * @return a set of all feature tuples involved in the project
     * @throws DataAccessException if the features cannot be retrieved
     */

    Set<FeatureTuple> getFeatures();

    /**
     * Returns the set of {@link Feature} associated with the named covariate dataset.
     * @param variableName the name of the covariate variable whose features are required
     * @return a set of all features associated with the named covariate dataset
     * @throws DataAccessException if the features cannot be retrieved
     * @throws NullPointerException if the declared variable name is null
     */

    Set<Feature> getCovariateFeatures( String variableName );

    /**
     * Returns the set of {@link FeatureGroup} to evaluate, including singletons.
     * @return a set of all feature groups involved in the project
     */

    Set<FeatureGroup> getFeatureGroups();

    /**
     * Returns a set of singleton {@link FeatureGroup} for which statistics should not be published. These groups are
     * used to calculate summary statistics only. They were not declared, explicitly, as singletons.
     * @return a set of singleton feature groups whose statistics should not be published
     */

    Set<FeatureGroup> getFeatureGroupsForWhichStatisticsShouldNotBePublished();

    /**
     * @return the left variable
     */

    Variable getLeftVariable();

    /**
     * @return the right variable
     */

    Variable getRightVariable();

    /**
     * @return the baseline variable
     */

    Variable getBaselineVariable();

    /**
     * Returns the covarariate dataset for the named variable.
     * @param variableName the variable name
     * @return the covariate dataset
     * @throws IllegalArgumentException if the named covariate does not exist
     * @throws NullPointerException if the variable name is null
     */

    Dataset getCovariateDataset( String variableName );

    /**
     * @param orientation the side of data for which the ensemble labels are required
     * @return the ensemble member labels for the specified dataset orientation
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    SortedSet<String> getEnsembleLabels( DatasetOrientation orientation );

    /**
     * @param orientation the orientation of the data source
     * @return true if the data source uses gridded data, false otherwise
     */

    boolean usesGriddedData( DatasetOrientation orientation );

    /**
     * @param orientation the side of data
     * @return whether there is lenient upscaling enforced for the specified side of data
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    boolean isUpscalingLenient( DatasetOrientation orientation );

    /**
     * @return the earliest analysis duration, defaults to {@link TimeWindowOuter#DURATION_MIN}.
     */

    Duration getEarliestAnalysisDuration();

    /**
     * @return the latest analysis duration, defaults to {@link TimeWindowOuter#DURATION_MAX}.
     */

    Duration getLatestAnalysisDuration();

    /**
     * @return The earliest possible monthday in a season or null
     */

    MonthDay getStartOfSeason();

    /**
     * @return The latest possible monthday in a season or null
     */

    MonthDay getEndOfSeason();

    /**
     * Returns unique identifier for this project data
     * @return the project hash
     */

    String getHash();

    /**
     * @return the project identity
     */

    long getId();

    /**
     * @return whether there is a baseline
     */

    boolean hasBaseline();

    /**
     * @return whether there is a generated baseline
     */

    boolean hasGeneratedBaseline();

    /**
     * @return whether the project uses probability thresholds
     */

    boolean hasProbabilityThresholds();
}