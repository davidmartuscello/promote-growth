
## Promoting Growth of Stack Exchange Sites

Stack Exchange is a collection of question answering
websites with a unified structure and a single user base. The
goal for this project is to provide an application for the clients to
grow their new website based on target marketing and relevant
cross-site promotion. A client will use the application to identify
the potential existing users on other Stack Exchange sites and
Reddit and send them a message invitation to sign up on the
client’s new website. This will help to attract new users to visit
the clients’ website.

## File Descriptions

- **XMLfunctions.scala**: Uses the scala xml library to parse the stack exchange xml files and select, filter, and clean the data
- **ParseXML.scala**: Select the stack exchange websites to be analyzed and parse them using XMLfunctions.scala
- **CreateTFIDFdataframe.scala**:
- **ComputeUserSimilarity.scala**:




## Instructions to Run

Download jars for the Stanford CoreNLP Library and the Spark Wrapper for CoreNLP.
```
wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.9.1/stanford-corenlp-3.9.1.jar
wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.9.1/stanford-corenlp-3.9.1-models.jar
wget http://dl.bintray.com/spark-packages/maven/databricks/spark-corenlp/0.4.0-spark2.4-scala2.11/spark-corenlp-0.4.0-spark2.4-scala2.11.jar
```
Launch the spark 2.X shell while loading the jar files
```
spark2-shell --jars stanford-corenlp-3.9.1-models.jar,spark-corenlp-0.4.0-spark2.4-scala2.11.jar,stanford-corenlp-3.9.1.jar
```

Run the files in the following order using the "load:" command (e.g. :load /home/dm4350/project/promote-growth/XMLfunctions.scala)
```
:load /path/to/files/XMLfunctions.scala
:load /path/to/files/ParseXML.scala
:load /path/to/files/
:load /path/to/files/
:load /path/to/files/
```

sortedUsers.coalesce(1).write.csv("hdfs:///user/dm4350/project/coalesce1")

hdfs dfs -ls /user/dm4350/project/coalesce1
hdfs dfs -copyToLocal /user/dm4350/project/coalesce1/ /home/dm4350/project/



.
.
.
.

---------------------------------------------------------------
REFERENCE GUIDE:

## Project title
A little info about your project and/ or overview that explains **what** the project is about.

## Motivation
A short description of the motivation behind the creation and maintenance of the project. This should explain **why** the project exists.

## Build status
Build status of continus integration i.e. travis, appveyor etc. Ex. -

[![Build Status](https://travis-ci.org/akashnimare/foco.svg?branch=master)](https://travis-ci.org/akashnimare/foco)
[![Windows Build Status](https://ci.appveyor.com/api/projects/status/github/akashnimare/foco?branch=master&svg=true)](https://ci.appveyor.com/project/akashnimare/foco/branch/master)

## Code style
If you're using any code style like xo, standard etc. That will help others while contributing to your project. Ex. -

[![js-standard-style](https://img.shields.io/badge/code%20style-standard-brightgreen.svg?style=flat)](https://github.com/feross/standard)

## Screenshots
Include logo/demo screenshot etc.

## Tech/framework used
Ex. -

<b>Built with</b>
- [Electron](https://electron.atom.io)

## Features
What makes your project stand out?

## Code Example
Show what the library does as concisely as possible, developers should be able to figure out **how** your project solves their problem by looking at the code example. Make sure the API you are showing off is obvious, and that your code is short and concise.

## Installation
Provide step by step series of examples and explanations about how to get a development env running.

## API Reference

Depending on the size of the project, if it is small and simple enough the reference docs can be added to the README. For medium size to larger projects it is important to at least provide a link to where the API reference docs live.

## Tests
Describe and show how to run the tests with code examples.

## How to use?
If people like your project they’ll want to learn how they can use it. To do so include step by step guide to use your project.

## Contribute

Let people know how they can contribute into your project. A [contributing guideline](https://github.com/zulip/zulip-electron/blob/master/CONTRIBUTING.md) will be a big plus.

## Credits
Give proper credits. This could be a link to any repo which inspired you to build this project, any blogposts or links to people who contrbuted in this project.

#### Anything else that seems useful

## License
A short snippet describing the license (MIT, Apache etc)

MIT © [Yourname]()
