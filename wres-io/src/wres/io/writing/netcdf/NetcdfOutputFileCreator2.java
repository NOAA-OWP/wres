package wres.io.writing.netcdf;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.Array;
import ucar.ma2.ArrayChar;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayInt;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.FeatureDimension;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.writing.WriteException;
import wres.statistics.generated.GeometryTuple;

class NetcdfOutputFileCreator2
{
    private NetcdfOutputFileCreator2(){}

    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetcdfOutputFileCreator2.class);

    /** The _FillValue and missing_value to use when writing. */
    static final double DOUBLE_FILL_VALUE = 9.9692099683868690e+36d;

    /** The _FillValue and missing_value to use when writing. */
    private static final int INT_FILL_VALUE = -999;

    /** The length of string to use for each feature variable in the file. */
    private static final int FEATURE_STRING_LENGTH = 32;

    /** The length of strings to use for lid variable in the file. */
    private static final int FEATURE_TUPLE_STRING_LENGTH = FEATURE_STRING_LENGTH * 3 + 2;

    private static final String NOTE_REGARDING_UNITS_AND_GDAL = "Purposely avoided setting true units and standard_name attributes to maintain compatibility with GDAL/OGR 3.0.4 and QGIS 3.10.5.";

    static String create( ProjectConfig projectDeclaration,
                          Path targetPath,
                          Set<FeatureTuple> features,
                          TimeWindowOuter window,
                          ZonedDateTime analysisTime,
                          Collection<MetricVariable> metricVariables )
    {
        if ( targetPath.toFile().exists() )
        {
            throw new IllegalStateException( "Cannot write to "
                                             + targetPath
                                             + " because it already exists." );
        }

        LOGGER.debug( "About to create a new file at {}. Variables count={}",
                      targetPath,
                      metricVariables.size() );
        NetcdfFileWriter writer =
                NetcdfOutputFileCreator2.initializeBlob( projectDeclaration,
                                                         targetPath,
                                                         features,
                                                         window,
                                                         analysisTime,
                                                         metricVariables );
        ArrayInt.D1 duration = new ArrayInt.D1( 1,false );
        duration.set( 0, ( int ) window.getLatestLeadDuration().toMinutes() );

        try
        {
            writer.write( "time", duration );
            writer.flush();
        }
        catch ( InvalidRangeException | IOException e )
        {
            throw new WriteException( "The lead time could not be written to "
                                      + targetPath, e );
        }

        ArrayInt.D1 analysisMinutes = new ArrayInt.D1( 1, false );
        analysisMinutes.set( 0, ( int ) Duration.between( Instant.ofEpochSecond( 0 ),
                                                          analysisTime.toInstant() )
                                                .toMinutes() );

        try
        {
            writer.write( "analysis_time", analysisMinutes );
            writer.flush();
        }
        catch ( InvalidRangeException | IOException e )
        {
            throw new WriteException( "The analysis time could not be written to "
                                      + targetPath, e );
        }

        return writer.getNetcdfFile().getLocation();
    }


    /**
     * Create and set up dimensions of the NetCDF files for a given project.
     * @param targetPath The path into which to write.
     * @param features The features to write (netCDF lib needs it)
     * @param window The outermost time window (TODO: support N windows)
     * @param analysisTime A time to label as "analysis time" in the blob.
     * @param metricVariables The variables to add to this blob.
     * @return The writer created
     * @throws WriteException when something goes wrong when creating or writing
     * @throws NullPointerException when any non-primitive arg is null
     */

    private static NetcdfFileWriter initializeBlob( ProjectConfig projectDeclaration,
                                                    Path targetPath,
                                                    Set<FeatureTuple> features,
                                                    TimeWindowOuter window,
                                                    ZonedDateTime analysisTime,
                                                    Collection<MetricVariable> metricVariables )
    {
        Objects.requireNonNull( projectDeclaration );
        Objects.requireNonNull( features );
        Objects.requireNonNull( window );
        Objects.requireNonNull( analysisTime );
        Objects.requireNonNull( metricVariables );

        try
        {
            NetcdfFileWriter writer =
                    NetcdfFileWriter.createNew( NetcdfFileWriter.Version.netcdf3,
                                                targetPath.toString() );
            writer.addGlobalAttribute( "Conventions", "CF-1.6" );

            // featureType="profile" may be a better way to represent stats.
            writer.addGlobalAttribute( "featureType", "point" );
            NetcdfOutputFileCreator2.setDimensionsAndVariables( projectDeclaration,
                                                                writer,
                                                                features,
                                                                window,
                                                                analysisTime,
                                                                metricVariables );
            writer.flush();
            return writer;
        }
        catch ( IOException ioe )
        {
            throw new WriteException( "Failed to create netCDF file at "
                                      + targetPath, ioe );
        }
    }


    /**
     * Given a writer to use and the context needed, set up the dimensions of
     * the netcdf file. Calls create() which makes writer out of define mode.
     * @throws IllegalStateException when writer is not in define mode
     * @throws NullPointerException when any non-primitive arg is null
     * @throws WriteException when netCDF is whiny
     */

    private static void setDimensionsAndVariables( ProjectConfig projectDeclaration,
                                                   NetcdfFileWriter writer,
                                                   Set<FeatureTuple> features,
                                                   TimeWindowOuter window,
                                                   ZonedDateTime analysisTime,
                                                   Collection<MetricVariable> metricVariables )
    {
        Objects.requireNonNull( writer );
        Objects.requireNonNull( features );
        Objects.requireNonNull( window );
        Objects.requireNonNull( analysisTime );
        Objects.requireNonNull( metricVariables );

        if ( !writer.isDefineMode() )
        {
            throw new IllegalStateException( "The writer must be in define mode." );
        }

        Dimension featureDimension = writer.addDimension( null,
                                                          "geo_feature_tuple",
                                                          features.size() );
        Dimension timeDimension = writer.addDimension( null,
                                                       "time",
                                                       1 );
        // NetCDF 3 uses a second dimension for string variables (char[])
        Dimension unknownDimension = writer.addDimension( null,
                                                          "analysis_time",
                                                          1 );
        Dimension featureNameDimension = writer.addDimension( null,
                                                              "feature_name_string_len",
                                                              FEATURE_STRING_LENGTH );
        Dimension lidStringDimension = writer.addDimension( null,
                                                            "string7",
                                                            FEATURE_TUPLE_STRING_LENGTH );

        List<Dimension> stationDimension = List.of( featureDimension );


        Variable projection = writer.addVariable( null,
                                                  "ProjectionCoordinateSystem",
                                                  DataType.INT,
                                                  "" );
        Attribute projectionAttributeOne =
                new Attribute( "long_name", "CRS definition" );
        Attribute projectionAttributeTwo =
                new Attribute( "grid_mapping_name", "latitude_longitude" );
        projection.addAll( List.of( projectionAttributeOne,
                                    projectionAttributeTwo ) );

        Variable timeVariable = writer.addVariable( null,
                                                    "time",
                                                    DataType.INT,
                                                    List.of( timeDimension ) );

        // Important for GDAL/OGR 3.0.4 and QGIS 3.10.5: DON'T USE DURATION UNIT
        // These will cause OGR to error out when reading the dataset!
        //Attribute timeUnits = new Attribute( "units", "seconds since 1970-01-01T00:00:00Z" );
        //Attribute timeUnits = new Attribute( "units", "minutes since 1970-01-01 00:00:00 UTC" );
        Attribute unitsMeters = new Attribute( "units", "meters" );

        // Important for GDAL/OGR 3.0.4: DON'T USE STANDARD NAME TIME!
        //Attribute timeStandardName = new Attribute( "standard_name", "time" );
        Attribute timeLongName = new Attribute( "long_name", "Lead duration in minutes" );
        Attribute timeNotes = new Attribute( "notes", NOTE_REGARDING_UNITS_AND_GDAL );


        // It might be feasible to model data as profile featureType using Z.
        //Attribute timeStandardName = new Attribute( "standard_name", "altitude" );
        //Attribute timePositive = new Attribute( "positive", "up" );
        //Attribute timeAxis =  new Attribute( "axis", "Z" );

        timeVariable.addAll( List.of( unitsMeters, timeLongName, timeNotes ) );

        Variable latVariable = writer.addVariable( null,
                                                   "lat",
                                                   DataType.DOUBLE,
                                                   stationDimension );
        Attribute latUnits =
                new Attribute( "units", "degrees_north" );
        String latName = "latitude";
        Attribute latStandardName =
                new Attribute( "standard_name", latName );
        Attribute latLongName = new Attribute( "long_name", latName );
        Attribute latAxis =  new Attribute( "axis", "Y" );
        latVariable.addAll( List.of( latUnits
                ,latStandardName
                ,latLongName
                ,latAxis
        ) );
        NetcdfOutputFileCreator2.addNoDataAttributes( latVariable,
                                                      DOUBLE_FILL_VALUE );

        Variable lonVariable = writer.addVariable( null,
                                                   "lon",
                                                   DataType.DOUBLE,
                                                   stationDimension );
        Attribute lonUnits =
                new Attribute( "units", "degrees_east" );
        String lonName = "longitude";
        Attribute lonStandardName =
                new Attribute( "standard_name", lonName );
        Attribute lonLongName = new Attribute( "long_name", lonName );
        Attribute lonAxis =
                new Attribute( "axis", "X" );
        lonVariable.addAll( List.of( lonUnits
                ,lonStandardName
                ,lonLongName
                ,lonAxis
        ) );
        NetcdfOutputFileCreator2.addNoDataAttributes( lonVariable,
                                                      DOUBLE_FILL_VALUE );

        Attribute coordinates = new Attribute( "coordinates", "lon lat" );
        Attribute gridMapping = new Attribute( "grid_mapping", "ProjectionCoordinateSystem" );

        Variable featureIdVariable = writer.addVariable( null,
                                                         "feature_id",
                                                         DataType.INT,
                                                         stationDimension );
        featureIdVariable.addAll( List.of( coordinates, gridMapping ) );

        List<Dimension> lidDimensions = List.of( featureDimension,
                                                 lidStringDimension );
        Variable lidVariable = writer.addVariable( null,
                                                   "lid",
                                                   DataType.CHAR,
                                                   lidDimensions );
        Attribute lidLongName = new Attribute( "long_name", "The geographic feature names of left, right, and optionally baseline concatenated with underscore." );
        lidVariable.addAll( List.of( coordinates, gridMapping, lidLongName ) );

        List<Dimension> featureNameDimensions = List.of( featureDimension,
                                                         featureNameDimension );
        Variable leftFeatureNameVariable = writer.addVariable( null,
                                                               "left_feature_name",
                                                               DataType.CHAR,
                                                               featureNameDimensions );
        Attribute leftFeatureNameLongName = new Attribute( "long_name", "The name of the geographic feature from the left dataset." );
        String leftAuthority = NetcdfOutputFileCreator2.getLeftFeatureDimensionName( projectDeclaration );
        Attribute leftFeatureNameAuthority = new Attribute( "feature_name_authority", leftAuthority );
        leftFeatureNameVariable.addAll( List.of( coordinates,
                                                 gridMapping,
                                                 leftFeatureNameLongName,
                                                 leftFeatureNameAuthority ) );

        Variable rightFeatureNameVariable = writer.addVariable( null,
                                                                "right_feature_name",
                                                                DataType.CHAR,
                                                                featureNameDimensions );
        Attribute rightFeatureNameLongName = new Attribute( "long_name", "The name of the geographic feature from the right dataset." );
        String rightAuthority = NetcdfOutputFileCreator2.getRightFeatureDimensionName( projectDeclaration );
        Attribute rightFeatureNameAuthority = new Attribute( "feature_name_authority", rightAuthority );
        rightFeatureNameVariable.addAll( List.of( coordinates,
                                                  gridMapping,
                                                  rightFeatureNameLongName,
                                                  rightFeatureNameAuthority ) );

        Variable baselineFeatureNameVariable = writer.addVariable( null,
                                                                   "baseline_feature_name",
                                                                   DataType.CHAR,
                                                                   featureNameDimensions );
        Attribute baselineFeatureNameLongName = new Attribute( "long_name", "The name of the geographic feature from the baseline dataset." );
        String baselineAuthority = NetcdfOutputFileCreator2.getBaselineFeatureDimensionName( projectDeclaration );
        Attribute baselineFeatureNameAuthority = new Attribute( "feature_name_authority", baselineAuthority );
        baselineFeatureNameVariable.addAll( List.of( coordinates,
                                                     gridMapping,
                                                     baselineFeatureNameLongName,
                                                     baselineFeatureNameAuthority ) );

        Variable analysisTimeVariable = writer.addVariable( null,
                                                            "analysis_time",
                                                            DataType.INT,
                                                            List.of( unknownDimension ) );
        // https://www.unidata.ucar.edu/software/udunits/CHANGE_LOG implies
        // that since udunits 2.0.1 released in 2008, rfc3339 dates work.
        // Important for GDAL/OGR 3.0.4 and QGIS 3.10.5: DON'T USE TIME UNIT
        //Attribute timeUnitsAttribute =
        // new Attribute( "units", "minutes since 1970-01-01T00:00:00Z" );
        //Attribute timeUnitsAttribute =
        //        new Attribute( "units", "minutes since 1970-01-01 00:00:00 UTC" );
        Attribute analysisTimeName =
                new Attribute( "long_name",
                               "minutes since 1970-01-01T00:00:00Z" );
        Attribute analysisTimeNotes =
                new Attribute( "notes",
                               NOTE_REGARDING_UNITS_AND_GDAL );
        analysisTimeVariable.addAll( List.of( unitsMeters,
                                              analysisTimeName,
                                              analysisTimeNotes ) );


        // When going 2D, add the 2nd dimension here.
        // The "profile" or "timeSeries" featureType would be likeliest to work.
        List<Dimension> fooAndFeatureDimensions = List.of( featureDimension );
        for ( MetricVariable metricVariable : metricVariables )
        {
            String metricName = metricVariable.getName();
            Variable ncVariable =
                    NetcdfOutputFileCreator2.addDoubleVariable( writer,
                                                                metricName,
                                                                fooAndFeatureDimensions,
                                                                metricVariable.getAttributes() );
            ncVariable.addAttribute( gridMapping );
            ncVariable.addAttribute( coordinates );
            // When going 2D, you will need a time dimension.
            //Attribute coordinatesThree = new Attribute( "coordinates", "time lon lat" );
            //ncVariable.addAttribute( coordinatesThree );
        }


        try
        {
            writer.create();
            writer.flush();
        }
        catch ( IOException e )
        {
            throw new WriteException( "Creating netCDF at "
                                      + writer.getNetcdfFile().getLocation()
                                      + " failed: ", e );
        }

        List<Array> namesToWrite = features.stream()
                                           .map( NetcdfOutputFileCreator2::getFeatureTupleName )
                                           .map( s -> stringToD2( s,
                                                                  FEATURE_TUPLE_STRING_LENGTH ) )
                                           .collect( Collectors.toList() );

        LOGGER.debug( "Here are feature names to write: {}", namesToWrite );

        for ( int i = 0; i < namesToWrite.size(); i++ )
        {
            ArrayChar.D2 toWrite = (ArrayChar.D2) namesToWrite.get( i );
            int[] index = { i , 0 };

            try
            {
                LOGGER.debug( "About to call write on {}, {}, {}",
                              lidVariable, index, toWrite );
                writer.write( lidVariable, index, toWrite );
            }
            catch ( IOException | InvalidRangeException e )
            {
                throw new WriteException( "Writing netCDF at "
                                          + writer.getNetcdfFile().getLocation()
                                          + " failed: ", e );
            }
        }

        // Write the left, right, baseline names independently.
        int featureIndex = 0;

        for ( FeatureTuple featureTuple : features )
        {
            ArrayChar.D2 leftToWrite = stringToD2( featureTuple.getLeftName(),
                                                   FEATURE_STRING_LENGTH );
            ArrayChar.D2 rightToWrite = stringToD2( featureTuple.getRightName(),
                                                    FEATURE_STRING_LENGTH );
            ArrayChar.D2 baselineToWrite = null;
            if ( Objects.nonNull( featureTuple.getBaseline() ) )
            {
                baselineToWrite = stringToD2( featureTuple.getBaselineName(),
                                              FEATURE_STRING_LENGTH );
            }

            int[] index = { featureIndex , 0 };

            try
            {
                LOGGER.debug( "About to call write on {}, {}, {}",
                              leftFeatureNameVariable, index, leftToWrite );
                writer.write( leftFeatureNameVariable, index, leftToWrite );
                LOGGER.debug( "About to call write on {}, {}, {}",
                              rightFeatureNameVariable, index, rightToWrite );
                writer.write( rightFeatureNameVariable, index, rightToWrite );

                if ( Objects.nonNull( baselineToWrite ) )
                {
                    LOGGER.debug( "About to call write on {}, {}, {}",
                                  baselineFeatureNameVariable,
                                  index,
                                  baselineToWrite );
                    writer.write( baselineFeatureNameVariable,
                                  index,
                                  baselineToWrite );
                }
            }
            catch ( IOException | InvalidRangeException e )
            {
                throw new WriteException( "Writing netCDF at "
                                          + writer.getNetcdfFile().getLocation()
                                          + " failed: ", e );
            }

            featureIndex++;
        }

        FeatureTuple first = features.stream()
                                     .findFirst()
                                     .orElseThrow();

        LeftOrRightOrBaseline datasetWithGeo =
                NetcdfOutputFileCreator2.getMostCompleteGeo( first );

        if ( Objects.nonNull( datasetWithGeo ) )
        {
            LOGGER.info( "Found geometry available, writing geometry {} to netCDF {}",
                         first,
                         writer.getNetcdfFile()
                               .getLocation() );
            List<FeatureKey.GeoPoint> points = Collections.emptyList();
            Integer srid = null;

            if ( datasetWithGeo.equals( LeftOrRightOrBaseline.LEFT ) )
            {
                srid = first.getLeft()
                            .getSrid();
                points = features.stream()
                                 .map( FeatureTuple::getLeft )
                                 .map( FeatureKey::getWkt )
                                 .map( FeatureKey::getLonLatOrNullFromWkt )
                                 .collect( Collectors.toList() );
            }
            else if ( datasetWithGeo.equals( LeftOrRightOrBaseline.RIGHT ) )
            {
                srid = first.getRight()
                            .getSrid();
                points = features.stream()
                                 .map( FeatureTuple::getRight )
                                 .map( FeatureKey::getWkt )
                                 .map( FeatureKey::getLonLatOrNullFromWkt )
                                 .collect( Collectors.toList() );
            }
            else if ( datasetWithGeo.equals( LeftOrRightOrBaseline.BASELINE ) )
            {
                srid = first.getBaseline()
                            .getSrid();
                points = features.stream()
                                 .map( FeatureTuple::getBaseline )
                                 .map( FeatureKey::getWkt )
                                 .map( FeatureKey::getLonLatOrNullFromWkt )
                                 .collect( Collectors.toList() );
            }

            LOGGER.debug( "EPSG srid={}", srid );

            // TODO: query spatialreference.org
            //   e.g. https://spatialreference.org/ref/epsg/4326/
            // All our SRIDs have to be EPSG SRIDs because we don't store the
            // authority.
            if ( Objects.nonNull( srid ) && srid.equals( 4326 ) )
            {
                // Example that plots in QGIS, build using ogr2ogr/gdal 3.0.4:
                //         char crs ;
                //                crs:grid_mapping_name = "latitude_longitude" ;
                //                crs:long_name = "CRS definition" ;
                //                crs:longitude_of_prime_meridian = 0. ;
                //                crs:semi_major_axis = 6378137. ;
                //                crs:inverse_flattening = 298.25722356300002502 ;
                //                crs:spatial_ref = "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AXIS[\"Latitude\",NORTH],AXIS[\"Longitude\",EAST],AUTHORITY[\"EPSG\",\"4326\"]]" ;

                String crsWktEpsg4326 = "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\","
                                        + "SPHEROID[\"WGS 84\",6378137,298.257223563,"
                                        + "AUTHORITY[\"EPSG\",\"7030\"]],"
                                        + "AUTHORITY[\"EPSG\",\"6326\"]],"
                                        + "PRIMEM[\"Greenwich\",0,"
                                        + "AUTHORITY[\"EPSG\",\"8901\"]],"
                                        + "UNIT[\"degree\",0.01745329251994328,"
                                        + "AUTHORITY[\"EPSG\",\"9122\"]],"
                                        //+ "AXIS[\"Latitude\",NORTH],AXIS[\"Longitude\",EAST],"
                                        + "AUTHORITY[\"EPSG\",\"4326\"]]";
                double lonPrimeMeridian = 0.0;
                double semiMajorAxis = 6378137.0;
                double inverseFlattening = 298.257223563;

                try
                {
                    writer.setRedefineMode( true );
                }
                catch ( IOException ioe )
                {
                    throw new WriteException( "Failed to set redefine mode to write an EPSG SRID in "
                                              + writer.getNetcdfFile(), ioe );
                }

                // spatial_ref is for GDAL:
                Attribute crsAttribute =
                        new Attribute( "spatial_ref", crsWktEpsg4326 );
                Attribute primeAttribute = new Attribute( "longitude_of_prime_meridian",
                                                          lonPrimeMeridian );
                Attribute semiMajorAttribute = new Attribute( "semi_major_axis",
                                                              semiMajorAxis );
                Attribute flatteningAttribute = new Attribute( "inverse_flattening",
                                                               inverseFlattening );
                Attribute proj4Attribute = new Attribute( "proj4", "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs" );
                projection.addAll( List.of( crsAttribute,
                                            primeAttribute,
                                            semiMajorAttribute,
                                            flatteningAttribute,
                                            proj4Attribute ) );

                try
                {
                    writer.setRedefineMode( false );
                    writer.flush();
                }
                catch ( IOException ioe )
                {
                    throw new WriteException( "Failed to write an EPSG SRID to "
                                              + writer.getNetcdfFile(), ioe );
                }
            }

            // We are using the position in the array to match lon/lat with the
            // features themselves, thus null needs to be preserved.
            for ( int i = 0; i < points.size(); i++ )
            {
                FeatureKey.GeoPoint point = points.get( i );

                if ( Objects.nonNull( point ) )
                {
                    int[] index = { i };
                    Array ncArrayX = ArrayDouble.D1.makeFromJavaArray( new double[] { point.getX() } );
                    Array ncArrayY = ArrayDouble.D1.makeFromJavaArray( new double[] { point.getY() } );

                    try
                    {
                        writer.write( lonVariable, index, ncArrayX );
                        writer.write( latVariable, index, ncArrayY );
                    }
                    catch ( InvalidRangeException | IOException e )
                    {
                        throw new WriteException( "Failed to write geometry X/Y point metadata in "
                                                  + writer.getNetcdfFile()
                                                          .getLocation()
                                                  + " for x=" + point.getX()
                                                  + " for y=" + point.getY() ,
                                                  e );
                    }
                }
            }
        }
        else
        {
            LOGGER.info( "No geometry available, not writing geometry to netCDF {}",
                         writer.getNetcdfFile()
                               .getLocation() );
        }

        LeftOrRightOrBaseline nwmDataset =
                NetcdfOutputFileCreator2.getFeatureDimension( projectDeclaration,
                                                              FeatureDimension.NWM_FEATURE_ID );

        if ( Objects.nonNull( nwmDataset ) )
        {
            int index = 0;

            for ( FeatureTuple featureTuple : features )
            {
                String name = featureTuple.getNameFor( nwmDataset );
                int nwmFeatureId;

                try
                {
                    nwmFeatureId = Integer.parseInt( name );
                }
                catch ( NumberFormatException nfe )
                {
                    throw new WriteException( "While writing NWM feature IDs to "
                                              + writer.getNetcdfFile()
                                                      .getLocation()
                                              + " encountered feature named '"
                                              + name
                                              + "' from the "
                                              + nwmDataset.value()
                                              + " dataset that was not an "
                                              + "integer, but NWM feature IDs "
                                              + "are assumed to be integers.",
                                              nfe );
                }

                ArrayInt.D1 ncValue = new ArrayInt.D1( 1, false );
                ncValue.set( 0, nwmFeatureId );

                try
                {
                    writer.write( featureIdVariable, new int[] { index }, ncValue );
                }
                catch ( InvalidRangeException | IOException e )
                {
                    throw new WriteException( "Failed to write NWM feature ID "
                                              + nwmFeatureId + " to "
                                              + writer.getNetcdfFile()
                                                      .getLocation(),
                                              e );
                }

                index++;
            }
        }
    }


    /**
     * Sets up common "no data" or "fill value" attributes according to CF
     * conventions:
     * http://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/cf-conventions.html#missing-data
     * Expected to be called exactly once per variable (not idempotent)
     * @param variable the variable to set the nodata value on, to mutate the
     *                 underlying NetCDF file
     * @param noDataValue the "fill value" or "no data value" to use
     * @throws NullPointerException when any non-primitive arg is null
     * @throws IllegalArgumentException when noDataValue is set to 0.0
     * @throws IllegalStateException when writer not in define mode
     */

    private static void addNoDataAttributes( Variable variable,
                                             double noDataValue )
    {
        Objects.requireNonNull( variable );

        if ( variable.getDataType() != DataType.DOUBLE )
        {
            throw new IllegalArgumentException( "Specify a variable of type DOUBLE when passing double as second arg" );
        }

        if ( Double.compare( noDataValue, 0.0 ) == 0 )
        {
            throw new IllegalArgumentException(
                    "Specify a noDataValue other than 0.0" );
        }

        // Transform the simple double into what nc expects (0-dimensional array?)
        double[] noDataValues = { noDataValue };
        Array ncNoDataValues = ArrayDouble.D0.makeFromJavaArray( noDataValues );

        Attribute firstAttribute =
                new Attribute( "_FillValue", DataType.DOUBLE );
        firstAttribute.setValues( ncNoDataValues );
        variable.addAttribute( firstAttribute );

        Attribute secondAttribute =
                new Attribute( "missing_value", DataType.DOUBLE );
        secondAttribute.setValues( ncNoDataValues );
        variable.addAttribute( secondAttribute );
    }


    /**
     * Sets up common "no data" or "fill value" attributes according to CF
     * conventions:
     * http://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/cf-conventions.html#missing-data
     * Expected to be called exactly once per variable (not idempotent)
     * @param variable the variable to set the nodata value on, to mutate the
     *                 underlying NetCDF file
     * @param noDataValue the "fill value" or "no data value" to use
     * @throws NullPointerException when any non-primitive arg is null
     * @throws IllegalStateException when writer not in define mode
     */

    private static void addNoDataAttributes( Variable variable,
                                             int noDataValue )
    {
        Objects.requireNonNull( variable );

        if ( variable.getDataType() != DataType.INT )
        {
            throw new IllegalArgumentException( "Specify a variable of type INT when passing int as second arg" );
        }

        // Transform the simple double into what nc expects (0-dimensional array?)
        int[] noDataValues = { noDataValue };
        Array ncNoDataValues = ArrayInt.D0.makeFromJavaArray( noDataValues );

        Attribute firstAttribute =
                new Attribute( "_FillValue", DataType.INT );
        firstAttribute.setValues( ncNoDataValues );
        variable.addAttribute( firstAttribute );

        Attribute secondAttribute =
                new Attribute( "missing_value", DataType.INT );
        secondAttribute.setValues( ncNoDataValues );
        variable.addAttribute( secondAttribute );
    }

    /**
     * Needs to stay consistent with below method.
     * @param geometryTuple The tuple from which to get a name.
     * @return The name as used in WRES netCDF output.
     */

    static String getGeometryTupleName( GeometryTuple geometryTuple )
    {
        String result = geometryTuple.getLeft()
                                     .getName()
                        + "_" + geometryTuple.getRight()
                                             .getName();

        if ( geometryTuple.hasBaseline() )
        {
            result += "_" + geometryTuple.getBaseline()
                                         .getName();
        }

        return result;
    }


    /**
     * Needs to stay consistent with above method.
     * @param featureTuple The feature tuple from which to get a name.
     * @return The name as used in WRES netCDF output.
     */
    static String getFeatureTupleName( FeatureTuple featureTuple )
    {
        String result = featureTuple.getLeftName()
                        + "_" + featureTuple.getRightName();

        if ( Objects.nonNull( featureTuple.getBaseline() ) )
        {
            result += "_" + featureTuple.getBaselineName();
        }

        return result;
    }


    /**
     * Look for which dataset uses the given dimension, if any, null otherwise.
     * If multiple use this feature dimension, return in priority order:
     * right, left, baseline
     * @param projectConfig Project declaration
     * @param featureDimension The feature dimension to seek
     * @return The l/r/b having given dimension, or null if none found.
     */

    static LeftOrRightOrBaseline getFeatureDimension( ProjectConfig projectConfig,
                                                      FeatureDimension featureDimension )
    {
        ProjectConfig.Inputs inputs = projectConfig.getInputs();
        DataSourceConfig right = inputs.getRight();
        FeatureDimension rightDimension = NetcdfOutputFileCreator2.getFeatureDimension( right );

        if ( Objects.nonNull( rightDimension )
             && rightDimension.equals( featureDimension ) )
        {
            return LeftOrRightOrBaseline.RIGHT;
        }

        DataSourceConfig left = inputs.getLeft();
        FeatureDimension leftDimension = NetcdfOutputFileCreator2.getFeatureDimension( left );

        if ( Objects.nonNull( leftDimension )
             && leftDimension.equals( featureDimension ) )
        {
            return LeftOrRightOrBaseline.LEFT;
        }

        DataSourceConfig baseline = inputs.getBaseline();

        if ( Objects.nonNull( baseline ) )
        {
            FeatureDimension baselineDimension = NetcdfOutputFileCreator2.getFeatureDimension( baseline );

            if ( Objects.nonNull( baselineDimension )
                 && baselineDimension.equals( featureDimension ) )
            {
                return LeftOrRightOrBaseline.BASELINE;
            }
        }

        return null;
    }

    /**
     * Get the left feature dimension name
     * @param projectConfig Project declaration
     * @return The dimension detected or custom if not consistent or not found.
     */

    private static String getLeftFeatureDimensionName( ProjectConfig projectConfig )
    {
        ProjectConfig.Inputs inputs = projectConfig.getInputs();
        DataSourceConfig left = inputs.getLeft();
        FeatureDimension leftDimension =
                NetcdfOutputFileCreator2.getFeatureDimension( left );
        if ( Objects.nonNull( leftDimension ) )
        {
            return leftDimension.value();
        }
        else
        {
            return FeatureDimension.CUSTOM.value();
        }
    }

    /**
     * Get the right feature dimension name
     * @param projectConfig Project declaration
     * @return The dimension detected or custom if not consistent or not found.
     */

    private static String getRightFeatureDimensionName( ProjectConfig projectConfig )
    {
        ProjectConfig.Inputs inputs = projectConfig.getInputs();
        DataSourceConfig right = inputs.getRight();
        FeatureDimension rightDimension =
                NetcdfOutputFileCreator2.getFeatureDimension( right );
        if ( Objects.nonNull( rightDimension ) )
        {
            return rightDimension.value();
        }
        else
        {
            return FeatureDimension.CUSTOM.value();
        }
    }


    /**
     * Get the baseline feature dimension name or empty String if no baseline.
     * @param projectConfig Project declaration
     * @return The dimension detected or custom if not consistent or not found,
     * empty string if no baseline.
     */

    private static String getBaselineFeatureDimensionName( ProjectConfig projectConfig )
    {
        ProjectConfig.Inputs inputs = projectConfig.getInputs();
        DataSourceConfig baseline = inputs.getBaseline();

        if ( Objects.isNull( baseline ) )
        {
            return "";
        }

        FeatureDimension baselineDimension =
                NetcdfOutputFileCreator2.getFeatureDimension( baseline );
        if ( Objects.nonNull( baselineDimension ) )
        {
            return baselineDimension.value();
        }
        else
        {
            return FeatureDimension.CUSTOM.value();
        }
    }


    /**
     * Get the feature dimension for a given DataSourceConfig, null if none or
     * inconsistent data.
     */

    static FeatureDimension getFeatureDimension( DataSourceConfig config )
    {
        Objects.requireNonNull( config );
        List<DataSourceConfig.Source> sources = config.getSource();
        List<InterfaceShortHand> interfaces = sources.stream()
                                                     .map( DataSourceConfig.Source::getInterface )
                                                     .filter( Objects::nonNull )
                                                     .collect( Collectors.toList() );
        Set<InterfaceShortHand> distinctInterfaces = new HashSet<>( interfaces );

        if ( interfaces.size() == sources.size()
             && distinctInterfaces.size() == 1 )
        {
            // OK, unanimous source declaration, detected it, it's first in set.
            for ( InterfaceShortHand interfaceShortHand : distinctInterfaces )
            {
                if ( interfaceShortHand.value()
                                       .startsWith( "nwm" )
                     || interfaceShortHand.value()
                                          .endsWith( "nwm" ) )
                {
                    return FeatureDimension.NWM_FEATURE_ID;
                }
                else if ( interfaceShortHand.value()
                                            .startsWith( "usgs" )
                          || interfaceShortHand.value()
                                               .endsWith( "usgs" ) )
                {
                    return FeatureDimension.USGS_SITE_CODE;
                }
                else if ( interfaceShortHand.value()
                                            .startsWith( "ahps" )
                          || interfaceShortHand.value()
                                               .endsWith( "ahps" ) )
                {
                    return FeatureDimension.NWS_LID;
                }

                // One-item loop. No more after first.
                break;
            }
        }
        else if ( distinctInterfaces.size() == 0
                  && Objects.nonNull( config.getFeatureDimension() ) )
        {
            return config.getFeatureDimension();
        }

        // No valid dimension found for the whole dataset.
        return null;
    }


    /**
     * Look for which geometry is most complete. Null if none of them have any.
     * Needs to stay consistent with below method.
     * @param geometryTuple The geometry group to search.
     * @return null if none have any geo info, otherwise best geo info
     */

    static LeftOrRightOrBaseline getMostCompleteGeo( GeometryTuple geometryTuple )
    {
        Objects.requireNonNull( geometryTuple );
        Objects.requireNonNull( geometryTuple.getLeft() );
        Objects.requireNonNull( geometryTuple.getRight() );
        String leftWkt = geometryTuple.getLeft()
                                      .getWkt();
        boolean leftHasSrid = geometryTuple.getLeft()
                                           .getSrid() > 0;
        String rightWkt = geometryTuple.getRight()
                                       .getWkt();
        boolean rightHasSrid = geometryTuple.getRight()
                                            .getSrid() > 0;
        String baselineWkt = null;
        boolean baselineHasSrid = false;

        if ( geometryTuple.hasBaseline() )
        {
            baselineWkt = geometryTuple.getBaseline()
                                       .getWkt();
            baselineHasSrid = geometryTuple.getBaseline()
                                           .getSrid() > 0;
        }

        return NetcdfOutputFileCreator2.findMostGeoData( leftWkt,
                                                         leftHasSrid,
                                                         rightWkt,
                                                         rightHasSrid,
                                                         baselineWkt,
                                                         baselineHasSrid );
    }


    /**
     * Look for which geometry is most complete, null if none of them have any.
     * Needs to stay consistent with above method.
     * @param featureTuple The geometry group to search.
     * @return null if none have any geo info, otherwise best geo info
     */

    static LeftOrRightOrBaseline getMostCompleteGeo( FeatureTuple featureTuple )
    {
        Objects.requireNonNull( featureTuple );
        Objects.requireNonNull( featureTuple.getLeft() );
        Objects.requireNonNull( featureTuple.getRight() );
        String leftWkt = featureTuple.getLeft()
                                     .getWkt();
        boolean leftHasSrid = Objects.nonNull( featureTuple.getLeft()
                                                           .getSrid() )
                              && featureTuple.getLeft()
                                             .getSrid() > 0;
        String rightWkt = featureTuple.getRight()
                                      .getWkt();
        boolean rightHasSrid = Objects.nonNull( featureTuple.getRight()
                                                            .getSrid() )
                               && featureTuple.getRight()
                                              .getSrid() > 0;
        String baselineWkt = null;
        boolean baselineHasSrid = false;

        if ( Objects.nonNull( featureTuple.getBaseline() ) )
        {
            baselineWkt = featureTuple.getBaseline()
                                      .getWkt();
            baselineHasSrid = Objects.nonNull( featureTuple.getBaseline()
                                                           .getSrid() )
                              && featureTuple.getBaseline()
                                             .getSrid() > 0;
        }

        return NetcdfOutputFileCreator2.findMostGeoData( leftWkt,
                                                         leftHasSrid,
                                                         rightWkt,
                                                         rightHasSrid,
                                                         baselineWkt,
                                                         baselineHasSrid );
    }

    private static LeftOrRightOrBaseline findMostGeoData( String leftWkt,
                                                          boolean leftHasSrid,
                                                          String rightWkt,
                                                          boolean rightHasSrid,
                                                          String baselineWkt,
                                                          boolean baselineHasSrid )
    {
        final String REGEX = " ";
        LeftOrRightOrBaseline winner = null;
        String[] leftElements = {};
        String[] rightElements = {};

        if ( Objects.nonNull( leftWkt ) )
        {
            leftElements = leftWkt.strip()
                                  .split( REGEX );
        }

        if ( Objects.nonNull( rightWkt ) )
        {
            rightElements = rightWkt.strip()
                                    .split( REGEX );
        }

        if ( leftElements.length > 0 || rightElements.length > 0 )
        {
            if ( !leftHasSrid && !rightHasSrid )
            {
                // Neither have SRID, choose whichever has more elements,
                // RIGHT if tie.
                if ( leftElements.length > rightElements.length )
                {
                    winner = LeftOrRightOrBaseline.LEFT;
                }
                else
                {
                    winner = LeftOrRightOrBaseline.RIGHT;
                }
            }
            else if ( rightHasSrid )
            {
                // Right has both SRID and lon/lat data
                winner = LeftOrRightOrBaseline.RIGHT;
            }
            else
            {
                // Left has both SRID and lon/lat data
                winner = LeftOrRightOrBaseline.LEFT;
            }
        }

        if ( baselineHasSrid || Objects.nonNull( baselineWkt ) )
        {
            if ( Objects.nonNull( baselineWkt ) )
            {
                String[] baselineElements = baselineWkt.strip()
                                                       .split( REGEX );

                if ( ( baselineElements.length > leftElements.length
                       || baselineElements.length > rightElements.length
                       || ( baselineHasSrid && !leftHasSrid
                            && !rightHasSrid && baselineElements.length > 1 ) ) )
                {
                    // Baseline only wins in narrow circumstances.
                    winner = LeftOrRightOrBaseline.BASELINE;
                }
            }
        }

        return winner;
    }


    /**
     * Add a netCDF variable of type double. Based on method in NetCDFCopier.
     * Also adds no-data and fill-value attributes.
     * @param fileWriter The file writer to use.
     * @param name The name of the variable to add.
     * @param dimensions The dimensions across which the variable lies.
     * @param attributes The attributes to add.
     */

    private static Variable addDoubleVariable( NetcdfFileWriter fileWriter,
                                               String name,
                                               List<Dimension> dimensions,
                                               Map<String,Object> attributes )
    {
        Variable variable = fileWriter.addVariable( name, DataType.DOUBLE, dimensions );
        NetcdfOutputFileCreator2.addNoDataAttributes( variable, DOUBLE_FILL_VALUE );

        for ( Map.Entry<String, Object> attribute : attributes.entrySet())
        {
            Attribute ncAttribute;

            if ( attribute.getValue() instanceof Number )
            {
                ncAttribute = new Attribute( attribute.getKey(),
                                             (Number) attribute.getValue() );
            }
            else
            {
                ncAttribute = new Attribute( attribute.getKey(),
                                             String.valueOf( attribute.getValue() ) );
            }

            variable.addAttribute( ncAttribute );
        }

        return variable;
    }

    /**
     * Convert a String to a ucar cdm netCDF 3 2D array.
     * @param string The string to convert.
     * @param length The length of the netCDF 3 array.
     * @return The ArrayChar.D2 version, truncated to length.
     */
    private static ArrayChar.D2 stringToD2( String string, int length)
    {
        char[] charArray = string.toCharArray();
        char[] truncatedCharArray = Arrays.copyOf( charArray, length );
        char[][] twoDimTruncatedCharArray = new char[][] { truncatedCharArray };
        return (ArrayChar.D2) ArrayChar.D2.makeFromJavaArray( twoDimTruncatedCharArray );
    }
}
