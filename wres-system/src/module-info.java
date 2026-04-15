/**
 * System settings module.
 */

module wres.system
{
    requires java.base;
    requires java.sql;
    requires jakarta.xml.bind;
    requires lombok;
    requires org.slf4j;
    requires org.apache.commons.lang3;
    requires com.sun.xml.fastinfoset;
    exports wres.system;
}
