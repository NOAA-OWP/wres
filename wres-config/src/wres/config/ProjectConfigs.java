package wres.config;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.config.generated.LeftOrRightOrBaseline.BASELINE;
import static wres.config.generated.LeftOrRightOrBaseline.LEFT;
import static wres.config.generated.LeftOrRightOrBaseline.RIGHT;

import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.ProjectConfig.Inputs;

/**
 * Provides static methods that help with ProjectConfig and its children.
 */

public class ProjectConfigs
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectConfigs.class );

    /**
     * Returns <code>true</code> if the input configuration has time-series metrics, otherwise <code>false</code>.
     * 
     * @param projectConfig the project configuration
     * @return true if the input configuration has time-series metrics, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasTimeSeriesMetrics( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project declaration." );

        return projectConfig.getMetrics()
                            .stream()
                            .anyMatch( next -> !next.getTimeSeriesMetric().isEmpty() );
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
     * <p>Returns the variable identifier from the inputs configuration. The identifier is one of the following in
     * order of precedent:</p>
     *
     * <p>If the variable identifier is required for the left and right:</p>
     * <ol>
     * <li>The label associated with the variable in the left source.</li>
     * <li>The label associated with the variable in the right source.</li>
     * <li>The value associated with the left variable.</li>
     * </ol>
     *
     * <p>If the variable identifier is required for the baseline:</p>
     * <ol>
     * <li>The label associated with the variable in the baseline source.</li>
     * <li>The value associated with the baseline variable.</li>
     * </ol>
     *
     * <p>In both cases, the last declaration is always present.</p>
     *
     * @param inputs the inputs configuration
     * @param isBaseline is true if the variable name is required for the baseline
     * @return the variable identifier
     * @throws IllegalArgumentException if the baseline variable is requested and the input does not contain
     *            a baseline source
     * @throws NullPointerException if the input is null
     */

    public static String getVariableIdFromProjectConfig( Inputs inputs, boolean isBaseline )
    {
        Objects.requireNonNull( inputs );

        // Baseline required?
        if ( isBaseline )
        {
            // Has a baseline source
            if ( Objects.nonNull( inputs.getBaseline() ) )
            {
                // Has a baseline source with a label
                if ( Objects.nonNull( inputs.getBaseline().getVariable().getLabel() ) )
                {
                    return inputs.getBaseline().getVariable().getLabel();
                }
                // Only has a baseline source with a variable value
                return inputs.getBaseline().getVariable().getValue();
            }
            throw new IllegalArgumentException( "Cannot identify the variable for the baseline as the input project "
                                                + "does not contain a baseline source." );
        }
        // Has a left source with a label
        if ( Objects.nonNull( inputs.getLeft().getVariable().getLabel() ) )
        {
            return inputs.getLeft().getVariable().getLabel();
        }
        // Has a right source with a label
        else if ( Objects.nonNull( inputs.getRight().getVariable().getLabel() ) )
        {
            return inputs.getRight().getVariable().getLabel();
        }
        // Has a left source with a variable value
        return inputs.getLeft().getVariable().getValue();
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

    private ProjectConfigs()
    {
        // Prevent construction, this is a static helper class.
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
     * Get all the graphical destinations from a configuration.
     *
     * @param config the config to search through
     * @return a list of graphical destinations
     * @throws NullPointerException when config is null
     */

    public static List<DestinationConfig> getGraphicalDestinations( ProjectConfig config )
    {
        return ProjectConfigs.getDestinationsOfType( config,
                                                     DestinationType.GRAPHIC,
                                                     DestinationType.PNG,
                                                     DestinationType.SVG );
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
     * Returns the first instance of the named metric configuration or null if no such configuration exists.
     * 
     * @param projectConfig the project configuration
     * @param metricName the metric name
     * @return the named metric configuration or null
     * @throws NullPointerException if one or both of the inputs are null
     */

    public static MetricConfig getMetricConfigByName( ProjectConfig projectConfig, MetricConfigName metricName )
    {
        Objects.requireNonNull( projectConfig, "Specify a non-null metric configuration as input." );
        Objects.requireNonNull( metricName, "Specify a non-null metric name as input." );

        for ( MetricsConfig next : projectConfig.getMetrics() )
        {
            Optional<MetricConfig> nextConfig =
                    next.getMetric().stream().filter( metric -> metric.getName().equals( metricName ) ).findFirst();
            if ( nextConfig.isPresent() )
            {
                return nextConfig.get();
            }
        }

        return null;
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
     * Given a declaration, add another source to it, returning the result.
     * The resulting String will be a WRES declaration. The original formatting
     * may be lost and comments may be stripped. Otherwise it should function
     * the same way as without calling this method, with the only difference
     * being additional data in the dataset.
     *
     * This method does parsing and writing of Strings. If you have an
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
     */

    public static ProjectConfig addSources( ProjectConfig oldDeclaration,
                                            List<DataSourceConfig.Source> leftSources,
                                            List<DataSourceConfig.Source> rightSources,
                                            List<DataSourceConfig.Source> baselineSources )
    {
        Objects.requireNonNull( leftSources );
        Objects.requireNonNull( rightSources );

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
                                      oldLeftDataset.getEnsemble(),
                                      oldLeftDataset.getTimeShift(),
                                      oldLeftDataset.getExistingTimeScale(),
                                      oldLeftDataset.getUrlParameter(),
                                      oldLeftDataset.getRemoveMemberByValidYear(),
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
                                      oldRightDataset.getEnsemble(),
                                      oldRightDataset.getTimeShift(),
                                      oldRightDataset.getExistingTimeScale(),
                                      oldRightDataset.getUrlParameter(),
                                      oldRightDataset.getRemoveMemberByValidYear(),
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
                                                  oldBaselineDataset.getEnsemble(),
                                                  oldBaselineDataset.getTimeShift(),
                                                  oldBaselineDataset.getExistingTimeScale(),
                                                  oldBaselineDataset.getUrlParameter(),
                                                  oldBaselineDataset.getRemoveMemberByValidYear(),
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
}
