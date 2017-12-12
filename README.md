# Rdb - connector - mysql

[ ![Codeship Status for emartech/rdb-connector-mysql](https://app.codeship.com/projects/7cc05880-9c55-0135-18fe-3ec7b5d83301/status?branch=master)](https://app.codeship.com/projects/252930)
[![](https://www.jitpack.io/v/emartech/rdb-connector-mysql.svg)](https://www.jitpack.io/#emartech/rdb-connector-mysql)

## Definitions:

**Router** - instantiates a specific type of connector
 
**Database connector** - implements an interface, so that the router can be connected to the specific type of database.

## Tasks:

Implements the general database connector trait, and contains mysql
 specific implementation. For testing, it uses the tests written in rdb - connector - common - test, applied for the mysql connector.

## Dependencies:

**[Rdb - connector - common](https://github.com/emartech/rdb-connector-common)** - defines a Connector trait, that should be implemented by different types of connectors. Contains the common logic, case classes and some default implementations, that may be overwritten by specific connectors, and may use functions implemented in the connectors. (eg. validation)


**[Rdb - connector - test](https://github.com/emartech/rdb-connector-test)**  - contains common test implementations, that may be used in specific connectors

## Testing:

After successfully installed docker, run the following commands in the root directory of the project:

```
brew cask install jet
docker-compose up
jet steps
```


