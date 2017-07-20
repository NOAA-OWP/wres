Most code in this module is generated during the build from xsd files.

It was not envisioned that java code would be committed to the repository in
this module, however, exceptions related to configuration probably belong here.

In order for xml file location information to be available at runtime, there are
two requirements:

1. The eclipselink moxy library on the classpath at runtime (see build.gradle)
2. A jaxb.properties file in the package directory of generated classes

For the second, see nonsrc/wres/config/generated for this file. It refers to the
class that will be used for JAXBContext at runtime.