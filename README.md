# Search Guard Suite Enterprise

![Logo](https://raw.githubusercontent.com/floragunncom/sg-assets/master/logo/sg_dlic_small.png)

## About this repository

This repository hosts the source code of the Search Guard Suite including both the community and enterprise features. 

Search Guard offers all basic security features for free. The community features of Search Guard can be used for all projects, including commercial projects, at absolutely no cost. Enterprise features require a [paid license](https://search-guard.com/licensing/) if you are using it for commercial purposes in a production environment. It is free of charge for non-commercial and academic use. 

### Community features

The files in the directories `ci`, `codova`, `dev`, `plugin`, `scheduler`, `security`, `sgadmin`, `signals`, `ssl`, `support` and in the root directory are part of the community edition and are **[Apache 2 licensed](http://www.apache.org/licenses/LICENSE-2.0)**. 

**If you are looking for a repository, which only contains the Apache 2 licensed files of Search Guard, go to the repository [Search Guard Suite](https://git.floragunn.com/search-guard/search-guard-suite).**

The Community Edition includes:

* Full data in transit encryption
* Node-to-node encryption
* Certificate revocation lists
* Role-based cluster level access control
* Role-based index level access control
* User-, role- and permission management
* Internal user database
* HTTP basic authentication
* PKI authentication
* Proxy authentication
* User Impersonation

### Enterprise features

Enterprise source code is located in the directories `dlic-auditlog`, `dlic-dlsfls`, `dlic-signals` and `dlic-security`. This code is **proprietarily licensed**; it is free of charge for non-commercial and academic use. For commercial use in a production environment you have to obtain a [paid license](https://search-guard.com/licensing/). We offer a [very flexible licensing model](https://search-guard.com/licensing/), based on productive clusters with an **unlimited number of nodes**. Non-productive systems like Development, Staging or QA are covered by the license at no additional cost.

The Enterprise Edition of Search Guard adds:

* Active Directory / LDAP
* Kerberos / SPNEGO
* JSON web token (JWT)
* OpenID
* SAML
* Document-level security
* Field-level security
* Audit logging 
* Compliance logging for GDPR, HIPAA, PCI, SOX and ISO compliance
* True Kibana multi-tenancy
* REST management API

Please see [here for a feature comparison](https://search-guard.com/product#feature-comparison).


## Documentation

Please refer to the [Official documentation](http://docs.search-guard.com) for detailed information on installing and configuring Search Guard.

## License

Copyright 2016-2022 by floragunn GmbH - All rights reserved

Unless required by applicable law or agreed to in writing, software
distributed here is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.


## Trial license

You can test all enterprise modules for 60 days. A trial license is automatically created when you first install Search Guard. You do not have to install the trial license manually. Just install Search Guard and you're good to go! 

## Support
* Commercial support available through [floragunn GmbH](https://search-guard.com)
* Community support available via [Search Guard Forum](https://forum.search-guard.com)
* Follow us on twitter [@searchguard](https://twitter.com/searchguard)


## Legal
floragunn GmbH is not affiliated with Elasticsearch BV.

Search Guard is a trademark of floragunn GmbH, registered in the U.S. and in other countries.

Elasticsearch, Kibana, Logstash, and Beats are trademarks of Elasticsearch BV, registered in the U.S. and in other countries.


