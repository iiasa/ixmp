   Copyright 2017 IIASA Energy Program

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


Introduction
============

This document specifies the guidelines for using the MESSAGEix framework and the ix modeling platform.
The aim of these guidelines is to ensure best scientific practice and collaborative development of the platform.

Documentation and further information is available at [www.iiasa.ac.at/message_ix](https://www.iiasa.ac.at/message_ix).
The framework is available for download at [www.github.com/iiasa/message_ix](http://www.github.com/iiasa/message_ix).
Please refer to the website and repository for the most up-to-date version of the code base.


User Guidelines
===============

A) Reference to the scientific publication and online resources
---------------------------------------------------------------

Please cite the following manuscript when using the MESSAGEix framework and/or the ix modeling platform 
for scientific publications or technical reports:

  Daniel Huppmann, Matthew Gidden, Oliver Fricko, Peter Kolp, 
  Clara Orthofer, Michael Pimmer, Keywan Riahi, and Volker Krey. 
  "The MESSAGEix Integrated Assessment Model and the ix modeling platform". 
  2017, in preparation. 

Also, please include a hyperlink to the online resource "https://www.iiasa.ac.at/message_ix" 
in any publication/report, or on a website describing your model or scientific analysis using the MESSAGEix framework.

B) Developing new model instances
---------------------------------

Researchers are welcome to develop new model instances using the MESSAGEix framework 
for their own research interests. However, any such model must be named "MESSAGEix xxx" or "MESSAGEix-xxx",
where 'xxx' is replaced by the name of the country/region, institutional affiliation or a similar identifying name.
For example, the national model for South Africa developed by Orthofer et al. [1] is called "MESSAGEix South Africa".

Furthermore, please ensure that there is no naming conflict with existing versions of the MESSAGEix model family.
When in doubt, please contact the IIASA Energy Program at "message_ix@iiasa.ac.at".

C) Notice of new publications
-----------------------------

We would appreciate a notice of publications using the MESSAGEix framework and the ix modeling platform.
Please send an e-mail to "message_ix@iiasa.ac.at".


Contributor Guidelines
======================

We appreciate contributions to the code base and development of new features for the framework.
Please use the GitHub "Issues" feature to raise questions concerning potential bugs or to propose new features,
but search for resolved/closed topics on similar subjects before raising a new issue.

For contributions to the code base of the platform, please use GitHub "Pull Requests", 
including a detailed description of the new feature and unit tests to illustrate the intended functionality.
All pull requests will be reviewed by the message_ix maintainers and/or contributors.

Contributors are required to sign the [Contributor License Agreement](CONTRIBUTOR_LICENSE.txt)
before any pull request can be reviewed. This ensures that all future users can benefit
from your contribution, and that your contributions do not infringe on anyone else's rights.
The electronic signature is collected via the [cla-assistant](https://github.com/cla-assistant/)
when issuing the pull request.

Code submitted via pull requests must adhere to the following style formats:
 - Python: [pep8](https://www.python.org/dev/peps/pep-0008/)
 - R: please follow the style of the existing code base
 - other (file names, CLI, etc.): please follow the style of the existing code base


References
==========

[1] Clara Orthofer, Daniel Huppmann, and Volker Krey. "South Africa's shale gas resources - chance or challenge?"
2017, in preparation.
