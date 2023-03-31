package wres.config.yaml.components;

import wres.config.yaml.DeclarationFactory;

/**
 * The type of threshold.
 * @author James Brown
 */
public enum ThresholdType
{
    /** Probability threshold. */
    PROBABILITY,
    /** Probability classifier threshold. */
    PROBABILITY_CLASSIFIER,
    /** Value threshold. */
    VALUE;

    /**
     * @return whether the threshold type is a probability
     */
    public boolean isProbability()
    {
        return this.name()
                   .startsWith( "PR" );
    }

    @Override
    public String toString()
    {
        return DeclarationFactory.fromEnumName( this.name() );
    }
}