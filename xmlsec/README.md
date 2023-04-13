# OpenSAML library patch

The module contains a patched version of the OpenSAML library. The patch is needed because the library in version 4.0.1 uses the class
`java.lang.ref.Cleaner`. Unfortunately, it is not possible to run the code which uses the `java.lang.ref.Cleaner` class inside
ES plugin due to restrictions imposed by ES Security Manager. `The org.elasticsearch.secure_sm.ThreadPermission "modifyArbitraryThread"`
permission is required to use the `java.lang.ref.Cleaner`. Such permission cannot be granted to the ES plugin. Therefore, this module
contains the patched version of artefact `org.opensaml:opensaml-xmlsec-impl` which does not use `java.lang.ref.Cleaner`. Caching solution used
by the following classes
- `org.opensaml.xmlsec.signature.impl.X509CertificateImpl`
- `org.opensaml.xmlsec.signature.impl.X509CRLImpl`

was reimplemented so that usage of class `java.lang.ref.Cleaner` is no longer required. The new cache mechanism utilizes the  
`java.lang.String.intern` method. 

# Module removal

The module should be removed when
- ES will not use Security Manager
- OpenSAML will not use `java.lang.ref.Cleaner`
- Java with noop Security Manager will be used by ES (JDK 17 or 18)