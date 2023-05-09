package wres.config.xml;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.config.xml.generated.LeftOrRightOrBaseline.BASELINE;
import static wres.config.xml.generated.LeftOrRightOrBaseline.LEFT;
import static wres.config.xml.generated.LeftOrRightOrBaseline.RIGHT;

import wres.config.xml.generated.DataSourceBaselineConfig;
import wres.config.xml.generated.DataSourceConfig;
import wres.config.xml.generated.DatasourceType;
import wres.config.xml.generated.DestinationConfig;
import wres.config.xml.generated.DestinationType;
import wres.config.xml.generated.LeftOrRightOrBaseline;
import wres.config.xml.generated.ProjectConfig;
import wres.config.xml.generated.ThresholdOperator;
import wres.config.xml.generated.TimeScaleConfig;
import wres.config.xml.generated.ProjectConfig.Inputs;

/**
 * Provides static methods that help with ProjectConfig and its children.
 */

public class ProjectConfigs
{

    private static final String SPECIFY_NON_NULL_PROJECT_DECLARATION = "Specify non-null project declaration.";
    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectConfigs.class );

    /**
     * Attempts to read a project declaration string or a path to a declaration string.
     * @param declarationStringOrPath the declaration string or path
     * @return the project declaration
     * @throws IOException if the declaration could not be read for any reason
     */

    public static ProjectConfigPlus readDeclaration( String declarationStringOrPath ) throws IOException
    {
        ProjectConfigPlus projectConfigPlus;

        // Declaration passed directly as an argument
        if ( declarationStringOrPath.startsWith( "<?xml " ) )
        {
            // Successfully detected a project passed directly as an argument.
            try
            {
                projectConfigPlus = ProjectConfigPlus.from( declarationStringOrPath,
                                                            "command line argument" );
            }
            catch ( IOException ioe )
            {
                String message = "Failed to unmarshal project configuration from command line argument.";
                throw new IOException( message, ioe );
            }
        }
        else
        {
            Path configPath = Paths.get( declarationStringOrPath );

            try
            {
                // Unmarshal the configuration
                projectConfigPlus = ProjectConfigPlus.from( configPath );
            }
            catch ( IOException ioe )
            {
                String message = "Failed to unmarshal project configuration from "
                                 + configPath;
                throw new IOException( message, ioe );
            }
        }

        return projectConfigPlus;
    }

    /**
     * Returns <code>true</code> if the input configuration has legacy CSV declared, otherwise <code>false</code>.
     * 
     * @param projectConfig the project configuration
     * @return true if the input configuration has legacy CSV, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasLegacyCsv( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, SPECIFY_NON_NULL_PROJECT_DECLARATION );
        return projectConfig.getOutputs()
                            .getDestination()
                            .stream()
                            .anyMatch( next -> next.getType() == DestinationType.CSV
                                               || next.getType() == DestinationType.NUMERIC );
    }

    /**
     * Returns <code>true</code> if the input configuration has pooling windows, otherwise <code>false</code>.
     * 
     * @param projectConfig the project configuration
     * @return true if the input configuration has pooling windows, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasPoolingWindows( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, SPECIFY_NON_NULL_PROJECT_DECLARATION );

        return Objects.nonNull( projectConfig.getPair().getIssuedDatesPoolingWindow() )
               || Objects.nonNull( projectConfig.getPair().getValidDatesPoolingWindow() );
    }

    /**
     * Compares the input instances of {@link ProjectConfig}. Returns a negative, zero, or positive value when the first
     * input is less than, equal to, or greater than the second input, respectively. This is a minimal implementation
     * that is consistent with {@link Object#equals(Object)} and otherwise compares the inputs according to the value
     * of the {@link ProjectConfig#getName()} alone.
     * 
     * @param first the first input
     * @param second the second input
     * @return a negative, zero or positive integer when the first input is less than, equal to, or greater than
     *            the second input
     * @throws NullPointerException if either input is null
     */

    public static int compare( ProjectConfig first, ProjectConfig second )
    {
        Objects.requireNonNull( first, "The first input is null, which is not allowed." );

        Objects.requireNonNull( second, "The second input is null, which is not allowed." );

        if ( first.equals( second ) )
        {
            return 0;
        }

        // Null friendly natural order on project name
        return Objects.compare( first.getName(), second.getName(), Comparator.nullsFirst( Comparator.naturalOrder() ) );
    }

    /**
     * Get a duration of a period from a timescale config
     * 
     * @param timeScaleConfig the config
     * @return the duration
     * @throws NullPointerException if the input is null or expected contents is null
     */

    public static Duration getDurationFromTimeScale( TimeScaleConfig timeScaleConfig )
    {
        Objects.requireNonNull( timeScaleConfig, "Specify non-null input configuration " );

        Objects.requireNonNull( timeScaleConfig.getUnit(),
                                "The unit associated with the time scale declaration was null, which is not allowed." );

        Objects.requireNonNull( timeScaleConfig.getPeriod(),
                                "The period associated with the time scale declaration was null, which is not allowed." );

        ChronoUnit unit = ChronoUnit.valueOf( timeScaleConfig.getUnit()
                                                             .value()
                                                             .toUpperCase() );
        return Duration.of( timeScaleConfig.getPeriod(), unit );
    }

    /**
     * <p>Returns the variable name from an {@link DataSourceConfig}. The identifier is one of the following in
     * order of precedent:</p>
    
     * <ol>
     * <li>The label associated with the variable.</li>
     * <li>The value associated with the variable.</li>
     * </ol>
     *
     * @param dataSourceConfig the data source configuration
     * @return the variable identifier
     * @throws NullPointerException if the input is null or the variable is undefined
     */

    public static String getVariableIdFromDataSourceConfig( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );
        Objects.requireNonNull( dataSourceConfig.getVariable() );

        if ( Objects.nonNull( dataSourceConfig.getVariable().getLabel() ) )
        {
            return dataSourceConfig.getVariable().getLabel();
        }

        return dataSourceConfig.getVariable().getValue();
    }

    /**
     * Get all the destinations from a configuration for a particular type.
     * @param config the config to search through
     * @param types the types to look for
     * @return a list of destinations with the type specified
     * @throws NullPointerException when config or type is null
     */

    public static List<DestinationConfig> getDestinationsOfType( ProjectConfig config,
                                                                 DestinationType... types )
    {
        Objects.requireNonNull( config, "Config must not be null." );
        Objects.requireNonNull( types, "Type must not be null." );

        List<DestinationConfig> result = new ArrayList<>();

        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            LOGGER.debug( "No destinations specified for config {}", config );
            return java.util.Collections.unmodifiableList( result );
        }

        for ( DestinationConfig d : config.getOutputs().getDestination() )
        {
            for ( DestinationType nextType : types )
            {
                if ( d.getType() == nextType )
                {
                    result.add( d );
                }
            }
        }

        return java.util.Collections.unmodifiableList( result );
    }

    /**
     * @param destinationType the destination type
     * @return Returns <code>true</code> if the input type is a graphical type, otherwise <code>false</code>
     */

    public static boolean isGraphicsType( DestinationType destinationType )
    {
        return destinationType == DestinationType.GRAPHIC || destinationType == DestinationType.PNG
               || destinationType == DestinationType.SVG;
    }

    /**
     * Get the declaration for a given dataset side, left or right or baseline.
     * @param projectConfig The project declaration to search.
     * @param side The side to return.
     * @return The declaration element for the left or right or baseline.
     */

    public static DataSourceConfig getDataSourceBySide( final ProjectConfig projectConfig,
                                                        final LeftOrRightOrBaseline side )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( side );
        Inputs inputs = projectConfig.getInputs();

        if ( side.equals( LEFT ) )
        {
            return inputs.getLeft();
        }
        else if ( side.equals( RIGHT ) )
        {
            return inputs.getRight();
        }
        else if ( side.equals( BASELINE ) )
        {
            return inputs.getBaseline();
        }
        else
        {
            throw new UnsupportedOperationException( "Unsupported side: " + side );
        }
    }

    /**
     * <p>Maps between a {@link ThresholdOperator} and a canonical
     * {@link wres.statistics.generated.Threshold.ThresholdOperator}.
     *
     * @param operator the declared threshold operator
     * @return the canonical threshold operator
     * @throws MetricConfigException if the operator is not mapped
     */

    public static wres.statistics.generated.Threshold.ThresholdOperator getThresholdOperator( ThresholdOperator operator )
    {
        Objects.requireNonNull( operator );

        return switch ( operator )
                {
                    case EQUAL_TO -> wres.statistics.generated.Threshold.ThresholdOperator.EQUAL;
                    case LESS_THAN -> wres.statistics.generated.Threshold.ThresholdOperator.LESS;
                    case GREATER_THAN -> wres.statistics.generated.Threshold.ThresholdOperator.GREATER;
                    case LESS_THAN_OR_EQUAL_TO -> wres.statistics.generated.Threshold.ThresholdOperator.LESS_EQUAL;
                    case GREATER_THAN_OR_EQUAL_TO ->
                            wres.statistics.generated.Threshold.ThresholdOperator.GREATER_EQUAL;
                };
    }

    /**
     * <p>Given a declaration, add another source to it, returning the result.
     * The resulting String will be a WRES declaration. The original formatting
     * may be lost and comments may be stripped. Otherwise it should function
     * the same way as without calling this method, with the only difference
     * being additional data in the dataset.
     *
     * <p>This method does parsing and writing of Strings. If you have an
     * already parsed declaration in a ProjectConfig, use the other addSource.
     *
     * @param rawDeclaration The original declaration String.
     * @param origin A named origin of the declaration for messages/exceptions.
     * @param leftSources The left sources to add to the declaration String.
     * @param rightSources The right sources to add to the declaration String.
     * @param baselineSources The right sources to add to the declaration String.
     * @return A new, modified declaration with the source added. Reformatted.
     * @throws IOException on failure to read the rawDeclaration
     * @throws JAXBException on failure to transform to the returned declaration
     * @throws IllegalArgumentException When adding baseline when no baseline.
     * @throws UnsupportedOperationException When code does not support side.
     * @throws ProjectConfigException When a required section is missing.
     */

    public static String addSources( String rawDeclaration,
                                     String origin,
                                     List<DataSourceConfig.Source> leftSources,
                                     List<DataSourceConfig.Source> rightSources,
                                     List<DataSourceConfig.Source> baselineSources )
            throws IOException, JAXBException
    {
        Objects.requireNonNull( rawDeclaration );
        Objects.requireNonNull( origin );
        Objects.requireNonNull( leftSources );
        Objects.requireNonNull( baselineSources );

        // Is this dangerous? Potentially raw user bytes being logged here.
        LOGGER.debug( "addSource original raw declaration: \n{}",
                      rawDeclaration );
        ProjectConfigPlus projectConfigPlus = ProjectConfigPlus.from( rawDeclaration,
                                                                      origin );
        ProjectConfig oldDeclaration = projectConfigPlus.getProjectConfig();
        LOGGER.debug( "addSource original (parsed) declaration: \n{}",
                      oldDeclaration );
        ProjectConfig newDeclaration = ProjectConfigs.addSources( oldDeclaration,
                                                                  leftSources,
                                                                  rightSources,
                                                                  baselineSources );
        JAXBElement<ProjectConfig> xsdAndJaxbAreWeird =
                new JAXBElement<>( new QName( "project" ),
                                   ProjectConfig.class,
                                   newDeclaration );
        JAXBContext jaxbContext = JAXBContext.newInstance( ProjectConfig.class );
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
        jaxbMarshaller.setProperty( Marshaller.JAXB_FORMATTED_OUTPUT,
                                    Boolean.TRUE );
        jaxbMarshaller.setProperty( Marshaller.JAXB_ENCODING, "UTF-8" );
        StringWriter stringWriter = new StringWriter();
        jaxbMarshaller.marshal( xsdAndJaxbAreWeird, stringWriter );
        LOGGER.debug( "Transformed into: \n{}", stringWriter );
        return stringWriter.toString();
    }

    /**
     * Given a declaration, add sources to it, returning the result.
     * The resulting ProjectConfig will be the original plus the source given.
     *
     * @param oldDeclaration The original declaration.
     * @param leftSources The left sources to add to the declaration String,
     *                    empty if none.
     * @param rightSources The right sources to add to the declaration String,
     *                     empty if none.
     * @param baselineSources The baseline sources to add to the declaration
     *                        String, null or empty if no baseline.
     * @return A new declaration based on the original with the source added.
     * @throws IllegalArgumentException When adding baseline when no baseline.
     * @throws UnsupportedOperationException When code does not support side.
     * @throws ProjectConfigException When required section is missing.
     */

    public static ProjectConfig addSources( ProjectConfig oldDeclaration,
                                            List<DataSourceConfig.Source> leftSources,
                                            List<DataSourceConfig.Source> rightSources,
                                            List<DataSourceConfig.Source> baselineSources )
    {
        Objects.requireNonNull( leftSources );
        Objects.requireNonNull( rightSources );

        if ( oldDeclaration.getInputs() == null )
        {
            throw new ProjectConfigException( oldDeclaration,
                                              "No <inputs> section found within"
                                                              + " the <project> declaration!" );
        }

        if ( oldDeclaration.getInputs()
                           .getLeft() == null )
        {
            throw new ProjectConfigException( oldDeclaration.getInputs(),
                                              "No <left> section found within "
                                                                          + "the <inputs> declaration!" );
        }

        if ( oldDeclaration.getInputs()
                           .getRight() == null )
        {
            throw new ProjectConfigException( oldDeclaration.getInputs(),
                                              "No <right> section found within "
                                                                          + "the <inputs> declaration!" );
        }

        if ( baselineSources != null
             && !baselineSources.isEmpty()
             && oldDeclaration.getInputs()
                              .getBaseline() == null )
        {
            throw new ProjectConfigException( oldDeclaration.getInputs(),
                                              "Unable to add to baseline because there was no baseline!" );
        }

        DataSourceConfig oldLeftDataset = oldDeclaration.getInputs()
                                                        .getLeft();
        List<DataSourceConfig.Source> newLeftSources =
                new ArrayList<>( oldLeftDataset.getSource() );
        newLeftSources.addAll( leftSources );
        DataSourceConfig newLeftDataset =
                new DataSourceConfig( oldLeftDataset.getType(),
                                      newLeftSources,
                                      oldLeftDataset.getVariable(),
                                      oldLeftDataset.getTransformation(),
                                      oldLeftDataset.getPersistence(),
                                      oldLeftDataset.getEnsemble(),
                                      oldLeftDataset.getTimeShift(),
                                      oldLeftDataset.getExistingTimeScale(),
                                      oldLeftDataset.getUrlParameter(),
                                      oldLeftDataset.getLabel(),
                                      oldLeftDataset.getFeatureDimension() );
        DataSourceConfig oldRightDataset = oldDeclaration.getInputs()
                                                         .getRight();
        List<DataSourceConfig.Source> newRightSources =
                new ArrayList<>( oldRightDataset.getSource() );
        newRightSources.addAll( rightSources );
        DataSourceConfig newRightDataset =
                new DataSourceConfig( oldRightDataset.getType(),
                                      newRightSources,
                                      oldRightDataset.getVariable(),
                                      oldRightDataset.getTransformation(),
                                      oldLeftDataset.getPersistence(),
                                      oldRightDataset.getEnsemble(),
                                      oldRightDataset.getTimeShift(),
                                      oldRightDataset.getExistingTimeScale(),
                                      oldRightDataset.getUrlParameter(),
                                      oldRightDataset.getLabel(),
                                      oldRightDataset.getFeatureDimension() );
        DataSourceBaselineConfig oldBaselineDataset = oldDeclaration.getInputs()
                                                                    .getBaseline();
        DataSourceBaselineConfig newBaselineDataset = null;
        if ( oldBaselineDataset != null && baselineSources != null )
        {
            List<DataSourceConfig.Source> newBaselineSources =
                    new ArrayList<>( oldBaselineDataset.getSource() );
            newBaselineSources.addAll( baselineSources );
            newBaselineDataset =
                    new DataSourceBaselineConfig( oldBaselineDataset.getType(),
                                                  newBaselineSources,
                                                  oldBaselineDataset.getVariable(),
                                                  oldBaselineDataset.getTransformation(),
                                                  oldLeftDataset.getPersistence(),
                                                  oldBaselineDataset.getEnsemble(),
                                                  oldBaselineDataset.getTimeShift(),
                                                  oldBaselineDataset.getExistingTimeScale(),
                                                  oldBaselineDataset.getUrlParameter(),
                                                  oldBaselineDataset.getLabel(),
                                                  oldBaselineDataset.getFeatureDimension(),
                                                  oldBaselineDataset.isSeparateMetrics() );
        }

        ProjectConfig.Inputs newInputs;
        newInputs = new ProjectConfig.Inputs( newLeftDataset,
                                              newRightDataset,
                                              newBaselineDataset );

        return new ProjectConfig( newInputs,
                                  oldDeclaration.getPair(),
                                  oldDeclaration.getMetrics(),
                                  oldDeclaration.getOutputs(),
                                  oldDeclaration.getLabel(),
                                  oldDeclaration.getName() );
    }


    /**
     * Returns whether the declared {@link DatasourceType} matches one of the forecast types, currently
     * {@link DatasourceType#SINGLE_VALUED_FORECASTS} and {@link DatasourceType#ENSEMBLE_FORECASTS}.
     * @param dataSourceConfig the configuration
     * @return true when the type of data is a forecast type
     */

    public static boolean isForecast( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );

        return dataSourceConfig.getType() == DatasourceType.SINGLE_VALUED_FORECASTS
               || dataSourceConfig.getType() == DatasourceType.ENSEMBLE_FORECASTS;
    }

    /**
     * Return <code>true</code> if the project contains ensemble forecasts, otherwise <code>false</code>.
     *
     * @param projectConfig the project declaration
     * @return whether or not the project contains ensemble forecasts
     * @throws NullPointerException if the projectConfig is null or the inputs declaration is null
     */

    public static boolean hasEnsembleForecasts( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getInputs() );

        Inputs inputs = projectConfig.getInputs();
        DataSourceConfig left = inputs.getLeft();
        DataSourceConfig right = inputs.getRight();
        DataSourceConfig baseline = inputs.getBaseline();

        if ( Objects.nonNull( left ) && left.getType() == DatasourceType.ENSEMBLE_FORECASTS )
        {
            return true;
        }

        if ( Objects.nonNull( right ) && right.getType() == DatasourceType.ENSEMBLE_FORECASTS )
        {
            return true;
        }

        return Objects.nonNull( baseline ) && baseline.getType() == DatasourceType.ENSEMBLE_FORECASTS;
    }

    /**
     * Returns <code>true</code> if a baseline is present, otherwise <code>false</code>.
     *
     * @param projectConfig the declaration to inspect
     * @return true if a baseline is present
     * @throws NullPointerException if the input is null
     */

    public static boolean hasBaseline( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        return Objects.nonNull( projectConfig.getInputs() )
               && Objects.nonNull( projectConfig.getInputs().getBaseline() );
    }

    private ProjectConfigs()
    {
        // Prevent construction, this is a static helper class.
    }
}
