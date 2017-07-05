Code in this module is generated during the build from xsd files.

It is not envisioned that java code will be committed to the repository in this
module.

In order for xml file location information to be available at runtime, there are
two requirements:

1. The eclipselink moxy library on the classpath at runtime (see build.gradle)
2. A jaxb.properties file in the package directory of generated classes

For the second, see nonsrc/wres/config/generated for this file. It refers to the
class that will be used for JAXBContext at runtime.