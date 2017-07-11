package wres.datamodel.metric;

import java.util.HashMap;
import java.util.Objects;

/**
 * A factory class for constructing {@link Metadata} and associated objects.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class DefaultMetadataFactory implements MetadataFactory
{

    /**
     * Instance of the factory.
     */

    private static MetadataFactory instance = null;

    /**
     * Cache of instances of {@link MetricOutputMetadata} by hash code. Typically, there will be relatively few unique
     * instances of {@link MetricOutputMetadata} that are common to a large number of {@link MetricOutput}. Caching
     * these instances will provide a substantial memory saving for a small CPU cost in retrieving them.
     */

    private final HashMap<Integer, MetricOutputMetadata> outputMetaCache;

    /**
     * For safety, set a limit on the number of objects allowed in the cache.
     */

    private final int outputMetaCacheLimit = 1000000;

    /**
     * Returns an instance of a {@link MetricOutputFactory}.
     * 
     * @return a {@link MetricOutputFactory}
     */

    public static MetadataFactory getInstance()
    {
        if(Objects.isNull(instance))
        {
            instance = new DefaultMetadataFactory();
        }
        return instance;
    }

    @Override
    public Metadata getMetadata(final int sampleSize)
    {
        return getMetadata(sampleSize, getDimension());
    }

    @Override
    public Metadata getMetadata(final int sampleSize, final Dimension dim)
    {
        return new MetadataImpl(sampleSize, dim, null, null, null);
    }

    @Override
    public Metadata getMetadata(final int sampleSize,
                                final Dimension dim,
                                final String geospatialID,
                                final String variableID,
                                final String scenarioID)
    {
        return new MetadataImpl(sampleSize, dim, geospatialID, variableID, scenarioID);
    }

    @Override
    public MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                  final Dimension dim,
                                                  final Dimension inputDim,
                                                  final MetricConstants metricID)
    {
        return getOutputMetadata(sampleSize, dim, inputDim, metricID, MetricConstants.MAIN, null, null, null, null);
    }

    @Override
    public MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                  final Dimension dim,
                                                  final Dimension inputDim,
                                                  final MetricConstants metricID,
                                                  final MetricConstants componentID)
    {
        return getOutputMetadata(sampleSize, dim, inputDim, metricID, componentID, null, null, null, null);
    }

    @Override
    public MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                  final Dimension dim,
                                                  final Dimension inputDim,
                                                  final MetricConstants metricID,
                                                  final MetricConstants componentID,
                                                  final String geospatialID,
                                                  final String variableID,
                                                  final String scenarioID,
                                                  final String baseScenarioID)
    {
        class MetricOutputMetadataImpl extends MetadataImpl implements MetricOutputMetadata
        {

            private MetricOutputMetadataImpl()
            {
                super(sampleSize, dim, geospatialID, variableID, scenarioID);
            }

            @Override
            public MetricConstants getMetricID()
            {
                return metricID;
            }

            @Override
            public MetricConstants getMetricComponentID()
            {
                return componentID;
            }

            @Override
            public String getScenarioIDForBaseline()
            {
                return baseScenarioID;
            }

            @Override
            public Dimension getInputDimension()
            {
                return inputDim;
            }

            @Override
            public boolean minimumEquals(final MetricOutputMetadata o)
            {
                return o.getMetricID() == getMetricID() && o.getMetricComponentID() == getMetricComponentID()
                    && o.getDimension().equals(getDimension()) && o.getInputDimension().equals(getInputDimension());
            }

            @Override
            public boolean equals(final Object o)
            {
                boolean returnMe = super.equals(o) && o instanceof MetricOutputMetadata;
                if(returnMe)
                {
                    final MetricOutputMetadata p = ((MetricOutputMetadata)o);
                    returnMe = p.getMetricID() == getMetricID() && p.getMetricComponentID() == getMetricComponentID()
                        && Objects.isNull(p.getScenarioIDForBaseline()) == Objects.isNull(getScenarioIDForBaseline())
                        && p.getInputDimension().equals(getInputDimension());
                    if(!Objects.isNull(getScenarioIDForBaseline()))
                    {
                        returnMe = returnMe && getScenarioIDForBaseline().equals(p.getScenarioIDForBaseline());
                    }
                }
                return returnMe;
            }

            @Override
            public int hashCode()
            {
                int returnMe = super.hashCode() + getInputDimension().hashCode() + getMetricID().hashCode()
                    + getMetricComponentID().hashCode() + Boolean.hashCode(Objects.isNull(getScenarioIDForBaseline()));
                if(!Objects.isNull(getScenarioIDForBaseline()))
                {
                    returnMe += getScenarioIDForBaseline().hashCode();
                }
                return returnMe;
            }

            @Override
            public String toString()
            {
                final StringBuilder b = new StringBuilder();
                b.append("[");
                if(!Objects.isNull(geospatialID))
                {
                    b.append(geospatialID).append(",");
                }
                if(!Objects.isNull(variableID))
                {
                    b.append(variableID).append(",");
                }
                if(!Objects.isNull(scenarioID))
                {
                    b.append(scenarioID).append(",");
                }
                if(!Objects.isNull(baseScenarioID))
                {
                    b.append(baseScenarioID).append(",");
                }
                b.append(sampleSize)
                 .append(",")
                 .append(dim)
                 .append(",")
                 .append(inputDim)
                 .append(",")
                 .append(metricID)
                 .append(",")
                 .append(componentID)
                 .append("]");
                return b.toString();
            }

        }
        //Return from cache if available
        final MetricOutputMetadata testMe = new MetricOutputMetadataImpl();
        final MetricOutputMetadata returnMe = outputMetaCache.get(testMe.hashCode());
        if(!Objects.isNull(returnMe))
        {
            return returnMe;
        }
        //Clear the cache if required
        if(outputMetaCache.size() > outputMetaCacheLimit)
        {
            outputMetaCache.clear();
        }
        //Increment the cache and return
        outputMetaCache.put(testMe.hashCode(), testMe);
        return testMe;
    }

    @Override
    public Dimension getDimension()
    {
        return getDimension("DIMENSIONLESS");
    }

    @Override
    public Dimension getDimension(final String dimension)
    {
        class DimensionImpl implements Dimension
        {
            /**
             * The dimension.
             */
            private final String dimension;

            public DimensionImpl(final String dimension)
            {
                Objects.requireNonNull("Specify a non-null dimension string.", dimension);
                this.dimension = dimension;
            }

            @Override
            public boolean hasDimension()
            {
                return dimension.equals("DIMENSIONLESS");
            }

            @Override
            public String getDimension()
            {
                return dimension;
            }

            @Override
            public boolean equals(final Object o)
            {
                return o instanceof Dimension && ((Dimension)o).hasDimension() == hasDimension()
                    && ((Dimension)o).getDimension().equals(getDimension());
            }

            @Override
            public int hashCode()
            {
                return Boolean.hashCode(hasDimension());
            }

            @Override
            public String toString()
            {
                return getDimension();
            }
        }
        return new DimensionImpl(dimension);
    }

    @Override
    public String getMetricName(final MetricConstants identifier)
    {
        switch(identifier)
        {
            case BRIER_SCORE:
                return "BRIER SCORE";
            case BRIER_SKILL_SCORE:
                return "BRIER SKILL SCORE";
            case CONTINGENCY_TABLE:
                return "CONTINGENCY TABLE";
            case CRITICAL_SUCCESS_INDEX:
                return "CRITICAL SUCCESS INDEX";
            case EQUITABLE_THREAT_SCORE:
                return "EQUITABLE THREAT SCORE";
            case MEAN_ABSOLUTE_ERROR:
                return "MEAN ABSOLUTE ERROR";
            case MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return "MEAN CONTINUOUS RANKED PROBABILITY SKILL SCORE";
            case MEAN_ERROR:
                return "MEAN_ERROR";
            case MEAN_SQUARE_ERROR:
                return "MEAN SQUARE ERROR";
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return "MEAN SQUARE ERROR SKILL SCORE";
            case PEIRCE_SKILL_SCORE:
                return "PEIRCE SKILL SCORE";
            case PROBABILITY_OF_DETECTION:
                return "PROBABILITY OF DETECTION";
            case PROBABILITY_OF_FALSE_DETECTION:
                return "PROBABILITY OF FALSE DETECTION";
            case ROOT_MEAN_SQUARE_ERROR:
                return "ROOT MEAN SQUARE ERROR";
            default:
                throw new IllegalArgumentException("Unable to determine the metric name from the prescribed "
                    + "identifier '" + identifier + "'.");
        }
    }

    @Override
    public String getMetricShortName(final MetricConstants identifier)
    {
        switch(identifier)
        {
            case BRIER_SCORE:
                return "BS";
            case BRIER_SKILL_SCORE:
                return "BSS";
            case CONTINGENCY_TABLE:
                return "CONTINGENCY TABLE";
            case CRITICAL_SUCCESS_INDEX:
                return "CSI";
            case EQUITABLE_THREAT_SCORE:
                return "ETS";
            case MEAN_ABSOLUTE_ERROR:
                return "MAE";
            case MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return "MEAN CRPSS";
            case MEAN_ERROR:
                return "MEAN_ERROR";
            case MEAN_SQUARE_ERROR:
                return "MSE";
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return "MSE-SS";
            case PEIRCE_SKILL_SCORE:
                return "PSS";
            case PROBABILITY_OF_DETECTION:
                return "PoD";
            case PROBABILITY_OF_FALSE_DETECTION:
                return "PoFD";
            case ROOT_MEAN_SQUARE_ERROR:
                return "RMSE";
            default:
                throw new IllegalArgumentException("Unable to determine the short metric name from the prescribed "
                    + "identifier '" + identifier + "'.");
        }
    }

    @Override
    public String getMetricComponentName(final MetricConstants identifier)
    {
        switch(identifier)
        {
            case MAIN:
                return "MAIN OUTPUT";
            case RELIABILITY:
                return "RELIABILITY";
            case RESOLUTION:
                return "RESOLUTION";
            case UNCERTAINTY:
                return "UNCERTAINTY";
            case TYPE_II_BIAS:
                return "TYPE II CONDITIONAL BIAS";
            case DISCRIMINATION:
                return "DISCRIMINATION";
            case SHARPNESS:
                return "SHARPNESS";
            case SAMPLE_SIZE:
                return "SAMPLE_SIZE";
            default:
                throw new IllegalArgumentException("Unable to determine the metric component name from the "
                    + "prescribed identifier '" + identifier + "'.");
        }
    }

    /**
     * Basic implementation of metadata.
     */

    private static class MetadataImpl implements Metadata
    {
        private final int sampleSize;
        private final Dimension dim;
        private final String geospatialID;
        private final String variableID;
        private final String scenarioID;

        private MetadataImpl(final int sampleSize,
                             final Dimension dim,
                             final String geospatialID,
                             final String variableID,
                             final String scenarioID)
        {
            this.sampleSize = sampleSize;
            this.dim = dim;
            this.geospatialID = geospatialID;
            this.variableID = variableID;
            this.scenarioID = scenarioID;
        }

        @Override
        public int getSampleSize()
        {
            return sampleSize;
        }

        @Override
        public Dimension getDimension()
        {
            return dim;
        }

        @Override
        public String getGeospatialID()
        {
            return geospatialID;
        }

        @Override
        public String getVariableID()
        {
            return variableID;
        }

        @Override
        public String getScenarioID()
        {
            return scenarioID;
        }

        @Override
        public boolean equals(final Object o)
        {
            boolean returnMe = o instanceof Metadata;
            if(returnMe)
            {
                final Metadata p = (Metadata)o;
                returnMe = p.getSampleSize() == getSampleSize() && p.getDimension().equals(getDimension());
                //Form a unique combined ID
                final StringBuilder b = new StringBuilder();
                b.append(getGeospatialID());
                b.append(getVariableID());
                b.append(getScenarioID());
                final StringBuilder c = new StringBuilder();
                c.append(p.getGeospatialID());
                c.append(p.getVariableID());
                c.append(p.getScenarioID());
                returnMe = returnMe && b.toString().equals(c.toString());
            }
            return returnMe;
        }

        @Override
        public int hashCode()
        {
            final int returnMe = Integer.hashCode(sampleSize) + getDimension().hashCode();
            //Form a unique combined ID
            final StringBuilder b = new StringBuilder();
            b.append(getGeospatialID());
            b.append(getVariableID());
            b.append(getScenarioID());
            return returnMe + b.toString().hashCode();
        }

        @Override
        public String toString()
        {
            final StringBuilder b = new StringBuilder();
            b.append("[");
            if(!Objects.isNull(geospatialID))
            {
                b.append(geospatialID).append(",");
            }
            if(!Objects.isNull(variableID))
            {
                b.append(variableID).append(",");
            }
            if(!Objects.isNull(scenarioID))
            {
                b.append(scenarioID).append(",");
            }
            b.append(sampleSize).append(",").append(dim).append("]");
            return b.toString();
        }
    }

    /**
     * No argument constructor.
     */

    private DefaultMetadataFactory()
    {
        outputMetaCache = new HashMap<>();
    };

}
