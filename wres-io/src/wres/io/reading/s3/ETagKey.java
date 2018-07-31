package wres.io.reading.s3;

import java.util.Objects;

class ETagKey implements Comparable<ETagKey>
{
    private final String etag;
    private final String key;

    ETagKey (final String eTag, final String key)
    {
        this.etag = eTag;
        this.key = key;
    }

    String getEtag()
    {
        return this.etag;
    }

    String getKey()
    {
        return this.key;
    }

    @Override
    public int compareTo( ETagKey eTagKey )
    {
        return this.getEtag().compareTo( eTagKey.getEtag() );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof ETagKey && this.getEtag().equals( ( ( ETagKey ) obj ).getEtag() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getEtag());
    }
}