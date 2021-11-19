package wres.io.thresholds;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.*;
import wres.datamodel.DataFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.config.ConfigHelper;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.csv.CSVThresholdReader;
import wres.io.thresholds.wrds.GeneralWRDSReader;
import wres.system.SystemSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExternalThresholdReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ExternalThresholdReader.class );

    private final SystemSettings systemSettings;
    private final ProjectConfig projectConfig;
    private final MetricsConfig metricsConfig;
    private final Set<FeatureTuple> features;
    private final UnitMapper desiredMeasurementUnitConverter;
    private final ThresholdBuilderCollection sharedBuilders;
    private final Set<FeatureTuple> recognizedFeatures = new HashSet<>();
    private final MeasurementUnit desiredMeasurementUnit;
    private final Set<String> unrecognizedThresholdFeatures = new TreeSet<>(); // Ordered

    public ExternalThresholdReader(
                                    final SystemSettings systemSettings,
                                    final ProjectConfig projectConfig,
                                    final MetricsConfig metricsConfig,
                                    final Set<FeatureTuple> features,
                                    final UnitMapper desiredMeasurementUnitConverter,
                                    final ThresholdBuilderCollection builders )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( metricsConfig );
        Objects.requireNonNull( features );
        Objects.requireNonNull( desiredMeasurementUnitConverter );
        Objects.requireNonNull( builders );

        this.systemSettings = systemSettings;
        this.projectConfig = projectConfig;
        this.metricsConfig = metricsConfig;
        this.features = features;
        this.desiredMeasurementUnitConverter = desiredMeasurementUnitConverter;
        this.sharedBuilders = builders;
        this.desiredMeasurementUnit =
                MeasurementUnit.of( this.desiredMeasurementUnitConverter.getDesiredMeasurementUnitName() );
    }

    public void read()
    {
        Set<MetricConstants> metrics = DataFactory.getMetricsFromMetricsConfig( this.metricsConfig,
                                                                                this.projectConfig );

        for ( ThresholdsConfig thresholdsConfig : this.getThresholds( this.metricsConfig ) )
        {
            Set<FeatureTuple> readFeatures = null;
            

            ThresholdFormat format = ExternalThresholdReader.getThresholdFormat( thresholdsConfig );
            if (format == ThresholdFormat.CSV)
            {
                readFeatures = readCSVThresholds( thresholdsConfig, metrics );
            }
            else if (format == ThresholdFormat.WRDS)
            {
                readFeatures = readWRDSThresholds( thresholdsConfig, metrics );    
            }
            else
            {
                String message = "The threshold format of " + format.toString() + " is not supported.";
                throw new IllegalArgumentException( message );
            }
            
            this.recognizedFeatures.addAll( readFeatures );
        }

        this.validate();
    }

    public Set<FeatureTuple> getRecognizedFeatures()
    {
        return Collections.unmodifiableSet( this.recognizedFeatures );
    }

    private Set<String> getUnrecognizedFeatureNames()
    {
        return Collections.unmodifiableSet( this.unrecognizedThresholdFeatures );
    }

    private Set<ThresholdsConfig> getThresholds( MetricsConfig metrics )
    {
        Set<ThresholdsConfig> thresholdsWithSources = new HashSet<>();

        for ( ThresholdsConfig thresholdsConfig : metrics.getThresholds() )
        {
            if ( thresholdsConfig.getCommaSeparatedValuesOrSource() instanceof ThresholdsConfig.Source )
            {
                thresholdsWithSources.add( thresholdsConfig );
            }
        }

        return thresholdsWithSources;
    }
    
    /**
     * @param dim Feature dimension.  For this method a custom dimension is assumed
     * to use NWS locations.
     * @param tupleSideName The feature name for the tuple side to check.
     * @param loc The WRDS location to test.
     * @return True if the tuple side name matches the WRDS location given the 
     * feature dimension of the tuple side.
     */
    private boolean doesFeatureNameForSideMatchWRDSLocation(FeatureDimension dim, String tupleSideName, WrdsLocation loc)
    {
        if ( (dim == FeatureDimension.USGS_SITE_CODE) && tupleSideName.equals(loc.getUsgsSiteCode() ) )
        {
            return true;
        }
        if ( (dim == FeatureDimension.NWM_FEATURE_ID) && tupleSideName.equals(loc.getNwmFeatureId() ) )
        {
            return true;
        }
        if ( ((dim == null) || (dim == FeatureDimension.NWS_LID) || (dim == FeatureDimension.CUSTOM)) 
                && tupleSideName.substring(0,5).equals(loc.getNwsLid()) ) //First 5 chars matter, only.
        {
            return true;
        }
        
        return false;
    }
    
    /**
     * @param loc The WRDS location to check.
     * @param feature The feature tuple to check against the WRDS location.
     * @return True if the WRDS location matches the feature tuple provided given all sides.
     */
    private boolean isWrdsLocationForFeatureTuple(WrdsLocation loc, FeatureTuple feature)
    {
        //I need the feature dimensions by evaluation side for later work.
        DataSourceConfig dataSourceConfig = ProjectConfigs.getDataSourceBySide( this.projectConfig, LeftOrRightOrBaseline.LEFT );
        FeatureDimension leftDim = ConfigHelper.getConcreteFeatureDimension( dataSourceConfig );
        dataSourceConfig = ProjectConfigs.getDataSourceBySide( this.projectConfig, LeftOrRightOrBaseline.RIGHT );
        FeatureDimension rightDim = ConfigHelper.getConcreteFeatureDimension( dataSourceConfig );
        dataSourceConfig = ProjectConfigs.getDataSourceBySide( this.projectConfig, LeftOrRightOrBaseline.BASELINE );
        FeatureDimension baselineDim = null;
        if (dataSourceConfig != null)
        {
            baselineDim = ConfigHelper.getConcreteFeatureDimension( dataSourceConfig );
        }
        
        if (!doesFeatureNameForSideMatchWRDSLocation(leftDim, feature.getLeftName(), loc))
        {
            return false;
        }
        
        if (!doesFeatureNameForSideMatchWRDSLocation(rightDim, feature.getRightName(), loc))
        {
            return false;
        }
        
        if (feature.getBaselineName() != null)
        {

            if (!doesFeatureNameForSideMatchWRDSLocation(baselineDim, feature.getBaselineName(), loc))
            {
                return false;
            }
        }
        
        return true;
    }
   

    /**
     * In addition to return the set, this also populates sharedBuilders with thresholds.
     * @param thresholdsConfig The declaration for the thresholds.
     * @param metrics The metric constants.
     * @return Set of feature tuples for which WRDS thresholds were found.
     */
    private Set<FeatureTuple> readWRDSThresholds( ThresholdsConfig thresholdsConfig, Set<MetricConstants> metrics )
    {
        Objects.requireNonNull( thresholdsConfig, "Specify non-null threshold configuration." );
        Objects.requireNonNull( metrics, "Specify non-null metrics." );

        ThresholdsConfig.Source source = (ThresholdsConfig.Source) thresholdsConfig.getCommaSeparatedValuesOrSource();
        LeftOrRightOrBaseline tupleSide = source.getFeatureNameFrom();
        DataSourceConfig dataSourceConfig = ProjectConfigs.getDataSourceBySide( this.projectConfig, tupleSide );
        FeatureDimension featureDimension = ConfigHelper.getConcreteFeatureDimension( dataSourceConfig );

        Set<FeatureTuple> recognized = new HashSet<>();

        // Threshold type: default to probability
        final ThresholdConstants.ThresholdGroup thresholdGroup;
        if ( Objects.nonNull( thresholdsConfig.getType() ) )
        {
            thresholdGroup = DataFactory.getThresholdGroup( thresholdsConfig.getType() );
        }
        else
        {
            thresholdGroup = ThresholdConstants.ThresholdGroup.PROBABILITY;
        }

        try
        {
            // Only 5 character handbook-5 ids are supported, so we need to ensure that feature names that
            // are passed are 5 characters long, at most, when passing to the reader if the user indicates
            // that it uses NWS LIDs
            final Function<FeatureTuple, String> identifyFeatureName;
            if ( featureDimension == FeatureDimension.NWS_LID )
            {
                identifyFeatureName = tuple -> tuple.getNameFor( tupleSide )
                                                    .substring(
                                                                0,
                                                                Math.min( tuple.getNameFor( tupleSide )
                                                                               .length(),
                                                                          5 ) );
            }
            else
            {
                identifyFeatureName = tuple -> tuple.getNameFor( tupleSide );
            }

            //Obtain the thresholds from WRDS using the feature name.  
            Map<WrdsLocation, Set<ThresholdOuter>> wrdsThresholds = GeneralWRDSReader.readThresholds(
                                                                                                      this.systemSettings,
                                                                                                      thresholdsConfig,
                                                                                                      this.desiredMeasurementUnitConverter,
                                                                                                      featureDimension,
                                                                                                      this.features.stream()
                                                                                                                   .map( identifyFeatureName )
                                                                                                                   .collect( Collectors.toSet() ) );

            //Add all of the WrdsLocation keys in the map to the unrecognized list.
	    //As we recognize each one, it will be removed from this list.
	    List<WrdsLocation>  unusedWrdsLocations = new ArrayList<>();
            unusedWrdsLocations.addAll(wrdsThresholds.keySet()); 

            //This will record a Map of FeatureTuple to Set<ThresholdOuter>
            //for the purposes of logging.
            Map<FeatureTuple, Set<ThresholdOuter>> tupleToThresholds = new HashMap<>();

            //Loop over the feature tuples finding the thresholds for each.
            for (FeatureTuple tuple : this.features)
            {

               //For the given tuple, identify the WRDS locations with thresholds
               //that match based solely on the input side.
               List<WrdsLocation> matchingWrdsLocations = new ArrayList<>();
               for ( Map.Entry<WrdsLocation, Set<ThresholdOuter>> thresholds : wrdsThresholds.entrySet() )
               {
                   final WrdsLocation location = thresholds.getKey();

                   if ( doesFeatureNameForSideMatchWRDSLocation(featureDimension, tuple.getNameFor(tupleSide), location) )
                   {
                       matchingWrdsLocations.add(location); 
                   }
               }
              
               //Setup a final list.  If no sets or one set were found
               //above, go with it.  If multiple sets were found, then 
               //use the other sides to help pare down the list further.
               List<WrdsLocation> finalList = new ArrayList<>();
               if (matchingWrdsLocations.size() > 1)
               {
                   LOGGER.warn("For tuple " + tuple + ", WRDS returned multiple candidate sets of "
                       + "thresholds.  Specifically, thresholds were found for the following "
                       + "WRDS locations that could apply: " + matchingWrdsLocations 
                       + ". That will be pared down using "
                       + "the feature information for other sides of the evaluation.");
                   for (WrdsLocation location : matchingWrdsLocations)
                   {
                       if (isWrdsLocationForFeatureTuple(location, tuple))
                       {
                           finalList.add(location);
                       }
                   }

                   //Error out if multiple sets of thresholds are still found.  
                   if (finalList.size() > 1)
                   {
                       throw new ThresholdReadingException("For feature tuple, " + tuple + ", we found "
                           + "multiple threshold sets from WRDS that could match after examining "
                           + "all sides.  That is not allowed. "
                           + "Consider using a different input side of the evaluation to request the "
                           + "thresholds or specifying additional thresholds.");
                   }
                   if (finalList.size() == 0)
                   {
                       LOGGER.warn("After paring down the thresholds for feature tuple " + tuple
                           + " no thresholds from WRDS were found to match the other sides of the "
                           + "evaluation, so no thresholds will be used.  You might want to use "
                           + "a different input side to request thresholds from WRDS.");
                   }
               }
               else
               {
                   finalList.addAll(matchingWrdsLocations);
               }

               //If a single set of thresholds is found for the tuple, then...
               if (finalList.size() == 1)
               {
                   //Record it for logging purposes.
                   tupleToThresholds.put(tuple, wrdsThresholds.get(finalList.get(0)));
                   
                   //Add to the recognized tuple list for a later message if necessary. 
                   recognized.add( tuple );

                   //Remove from the unused list of WRDS locations with thresholds.
                   unusedWrdsLocations.remove(finalList.get(0));

                   //Add the thresholds to the shared builder.
                   for ( MetricConstants metricName : metrics )
                   {
                       for ( ThresholdOuter threshold : wrdsThresholds.get(finalList.get(0)) )
                       {

// Used to investigate not all thresholds being used... System.err.println("####>> ADDING .. " + tuple + " ------- " + threshold );

                           this.sharedBuilders.addThreshold(
                                                             tuple,
                                                             thresholdGroup,
                                                             metricName,
                                                             threshold );
                       }
                   }
               }     
            }

            //Populate the unrecognized (i.e., unused) threshold 
            //features using the entire WrdsLocation toString.
            for (WrdsLocation loc : unusedWrdsLocations)
            {
                this.unrecognizedThresholdFeatures.add (loc.toString());
            }

            //Output a final log message.
            LOGGER.info("These thresholds were added for each of the following feature tuples: " 
                + tupleToThresholds);

            return Collections.unmodifiableSet( recognized );
        }
        catch ( IOException e )
        {
            throw new MetricConfigException( "Failed to read the external thresholds.", e );
        }
    }
    
    /**
     * Reads thresholds from a CSV file.
     * @param thresholdsConfig The threshold declaration.
     * @param metrics The metric constants.
     * @return Set of feature tuples with thresholds, those thresholds stored in this.sharedBuilders.
     */
    private Set<FeatureTuple> readCSVThresholds( ThresholdsConfig thresholdsConfig, Set<MetricConstants> metrics )
    {

        Objects.requireNonNull( thresholdsConfig, "Specify non-null threshold configuration." );

        Objects.requireNonNull( metrics, "Specify non-null metrics." );

        ThresholdsConfig.Source source = (ThresholdsConfig.Source) thresholdsConfig.getCommaSeparatedValuesOrSource();
        LeftOrRightOrBaseline tupleSide = source.getFeatureNameFrom();

        DataSourceConfig dataSourceConfig = ProjectConfigs.getDataSourceBySide( this.projectConfig, tupleSide );
        FeatureDimension featureDimension = ConfigHelper.getConcreteFeatureDimension( dataSourceConfig );

        Set<FeatureTuple> recognized = new HashSet<>();

        // Threshold type: default to probability
        final ThresholdConstants.ThresholdGroup thresholdGroup;
        if ( Objects.nonNull( thresholdsConfig.getType() ) )
        {
            thresholdGroup = DataFactory.getThresholdGroup( thresholdsConfig.getType() );
        }
        else
        {
            thresholdGroup = ThresholdConstants.ThresholdGroup.PROBABILITY;
        }

        try
        {
            Map<String, Set<ThresholdOuter>> readThresholds;
            ThresholdFormat format = ExternalThresholdReader.getThresholdFormat( thresholdsConfig );

            // If we're going with NWS_LIDs with WRDS, we want to do equality checks on the strict handbook-5s.
            // Some RFC data append an added identifier to the end of their handbook 5, preventing a match, so we
            // want to roll with equivalence based on those first five characters. To achieve this, we use a custom
            // function for the equivalency checks in the coming loop rather than a strict String::equals
            BiPredicate<String, String> equalityCheck = String::equals;

            readThresholds = CSVThresholdReader.readThresholds(
                                                               this.systemSettings,
                                                               thresholdsConfig,
                                                               this.getSourceMeasurementUnit( thresholdsConfig ),
                                                               this.desiredMeasurementUnitConverter );

            // Now that we have mappings between location identifiers and their thresholds,
            // try to match those up with our features
            for ( Map.Entry<String, Set<ThresholdOuter>> thresholds : readThresholds.entrySet() )
            {
                final String locationIdentifier = thresholds.getKey();

                // Try to find one of our configured features whose side matches what we were able to pluck out
                // from our threshold requests
                Optional<FeatureTuple> possibleFeature = this.features.stream()
                                                                      .filter( tuple -> equalityCheck.test( tuple.getNameFor( tupleSide ),
                                                                                                                 locationIdentifier ) )
                                                                      .findFirst();

                // If none were found, just move on. This might happen in the case where a CSV returns a mountain of
                // thresholds yet we only want a few
                if ( possibleFeature.isEmpty() )
                {
                    this.unrecognizedThresholdFeatures.add( locationIdentifier );
                    continue;
                }

                // Now that we know we have a match, add the feature to the list of features we know can be evaluated
                FeatureTuple feature = possibleFeature.get();
                recognized.add( feature );

                // Now that we have the feature, the metrics, and the thresholds, we can now add them to a
                // greater collection for use later
                for ( MetricConstants metricName : metrics )
                {
                    for ( ThresholdOuter threshold : thresholds.getValue() )
                    {
                        this.sharedBuilders.addThreshold(
                                                          feature,
                                                          thresholdGroup,
                                                          metricName,
                                                          threshold );
                    }
                }
            }

            return Collections.unmodifiableSet( recognized );
        }
        catch ( IOException e )
        {
            throw new MetricConfigException( "Failed to read the external thresholds.", e );
        }
    }

    /**
     * Creates a function to examine a WrdsLocation and extract the correct identifier
     *
     * @param side The side of the project's input to read identifiers from
     * @return a function that will retrieve the correct identifier from a WrdsLocation
     */
    private Function<WrdsLocation, String> getFeatureIdentifier( LeftOrRightOrBaseline side )
    {
        // Start out by getting the DataSourceConfig corresponding with the side; we'll need this in order to
        // try to figure out what field from the WrdsLocation we want to read from
        DataSourceConfig dataSourceConfig;

        switch ( side )
        {
            case RIGHT:
                dataSourceConfig = this.projectConfig.getInputs().getRight();
                break;
            case BASELINE:
                dataSourceConfig = this.projectConfig.getInputs().getBaseline();
                break;
            default:
                dataSourceConfig = this.projectConfig.getInputs().getLeft();
        }

        FeatureDimension dimension = ConfigHelper.getConcreteFeatureDimension( dataSourceConfig );

        if ( dimension == null || dimension == FeatureDimension.CUSTOM )
        {
            LOGGER.warn( "A definitive feature dimension could not be determined for linking thresholds to features; " +
                         "defaulting to the NWS lid" );
            return WrdsLocation::getNwsLid;
        }

        // Now that we know what dimension to use, we just have to return a function that will pluck the right
        // one off of the WrdsLocation. WRDS only supports three different formats and it's fairly obvious which
        // sources use NWM ids or USGS sites, not so much for NWS lids. Since what CAN use NWS lids is so vague,
        // we assume that as the base case.
        switch ( dimension )
        {
            case NWM_FEATURE_ID:
                return WrdsLocation::getNwmFeatureId;
            case USGS_SITE_CODE:
                return WrdsLocation::getUsgsSiteCode;
            default:
                return WrdsLocation::getNwsLid;
        }
    }

    public static ThresholdFormat getThresholdFormat( ThresholdsConfig config )
    {
        return ( (ThresholdsConfig.Source) config.getCommaSeparatedValuesOrSource() ).getFormat();
    }

    private MeasurementUnit getDesiredMeasurementUnit()
    {
        return this.desiredMeasurementUnit;
    }

    private MeasurementUnit getSourceMeasurementUnit( ThresholdsConfig config )
    {
        MeasurementUnit measurementUnit;

        ThresholdsConfig.Source source = (ThresholdsConfig.Source) config.getCommaSeparatedValuesOrSource();

        if ( source.getUnit() != null && !source.getUnit().isBlank() )
        {
            measurementUnit = MeasurementUnit.of( source.getUnit() );
        }
        else
        {
            measurementUnit = this.getDesiredMeasurementUnit();
        }

        return measurementUnit;
    }

    /**
     * Validates the thresholds against features. 
     */

    private void validate()
    {
        // No external thresholds declared
        if ( this.getRecognizedFeatures().isEmpty() && this.getUnrecognizedFeatureNames().isEmpty() )
        {
            LOGGER.debug( "No external thresholds to validate." );

            return;
        }

        LOGGER.debug( "Attempting to reconcile the {} features to evaluate with the {} features for which external "
                      + "thresholds are available.",
                      this.features.size(),
                      this.getRecognizedFeatures().size() );

        // Iterate the features to evaluate, filtering any for which external thresholds are not available
        Set<FeatureTuple> missingThresholds = this.features
                                                           .stream()
                                                           .filter( feature -> !this.getRecognizedFeatures()
                                                                                    .contains( feature ) )
                                                           .collect( Collectors.toSet() );

        if ( ( !missingThresholds.isEmpty() || !this.getUnrecognizedFeatureNames().isEmpty() )
             && LOGGER.isWarnEnabled() )
        {
            StringJoiner joiner = new StringJoiner( ", " );

            if ( missingThresholds.isEmpty() )
            {
                joiner.add( "[]" );
            }
            else
            {
                for ( FeatureTuple feature : missingThresholds )
                {
                    joiner.add( feature.toString() );
                }
            }

            int missingCount = missingThresholds.size() + this.unrecognizedThresholdFeatures.size();

            LOGGER.warn( "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
                         "While attempting to reconcile the features to ",
                         "evaluate with the features for which thresholds ",
                         "are available, found ",
                         this.features.size(),
                         " features to evaluate and ",
                         this.getRecognizedFeatures().size(),
                         " features for which thresholds were found, but ",
                         missingCount,
                         " features for which thresholds could not be ",
                         "reconciled with features to evaluate. Features without ",
                         "thresholds will be skipped. If the number of features ",
                         "without thresholds is larger than expected, ensure that ",
                         "the source of feature names (featureNameFrom) is properly ",
                         "declared for the external thresholds. The ",
                         "declared features without thresholds are: ",
                         joiner,
                         ". The feature names associated with thresholds for which no features were declared are: ",
                         this.getUnrecognizedFeatureNames() );
        }
        
        if ( missingThresholds.size() == this.features.size() )
        {
            throw new ThresholdReadingException( "Failed to discover any features for which thresholds were "
                                                 + "available from the external sources declared. Add some thresholds "
                                                 + "for one or more of the declared features, declare some features "
                                                 + "for which thresholds are available or remove the declaration of "
                                                 + "thresholds altogether. The names of features encountered without "
                                                 + "thresholds are "
                                                 + this.getUnrecognizedFeatureNames()
                                                 + ". Thresholds were not discovered for any of the following declared "
                                                 + "features "
                                                 + missingThresholds
                                                 + "." );
        }

        LOGGER.info( "Discovered {} features to evaluate for which external thresholds were available and {} "
                     + "features with external thresholds that could not be evaluated (e.g., because there was "
                     + "no data for these features).",
                     this.getRecognizedFeatures().size(),
                     this.getUnrecognizedFeatureNames().size() );
    }

}

