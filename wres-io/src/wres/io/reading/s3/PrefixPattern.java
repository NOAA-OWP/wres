package wres.io.reading.s3;

@Deprecated
class PrefixPattern implements Comparable<PrefixPattern>
{
    private final String prefix;
    private final String pattern;

    PrefixPattern( final String prefix, final String pattern )
    {
        this.prefix = prefix;
        this.pattern = pattern;
    }

    @Override
    public int compareTo( PrefixPattern prefixPattern )
    {
        int comparison = this.getPrefix().compareTo(prefixPattern.getPrefix());
        if (comparison == 0)
        {
            comparison = this.getPattern().compareTo( prefixPattern.getPattern() );
        }

        return comparison;
    }

    public String getPattern()
    {
        return pattern;
    }

    public String getPrefix()
    {
        return prefix;
    }
}
