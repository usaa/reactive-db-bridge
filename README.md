# Project
This project is an implementation of the [R2DBC 0.8.6 SPI](https://r2dbc.io/spec/0.8.6.RELEASE/spec/html/) that delegates all database operations to the [Vert.X Reactive DB2 Client](https://vertx.io/docs/vertx-db2-client/java/) (pulled in as a transitive dependency). This will enable the use of IBM DB2 and UDB databases in reactive applications using frameworks that build on the R2DBC specification (e.g. Spring Data).

## Background
R2DBC is an open standard for building Java drivers for SQL databases. It is the "reactive" counterpart to the JDBC (non-reactive / blocking) standard. There are R2DBC [drivers available](https://r2dbc.io/drivers/) for most major DB platforms (Oracle, MS-SQL, MySQL, PostgreSQL, ...), but particularly there is not a driver for IBM DB2/UDB. About 2 years ago, IBM created an empty GitHub repository [r2dbc-db2](https://github.com/IBM/r2dbc-db2) but has never done anything with it. Since then, IBM provided a reactive client driver as part of the Eclipse Vert.X project ([docs](https://vertx.io/docs/vertx-db2-client/java/) / [code](https://github.com/eclipse-vertx/vertx-sql-client/tree/master/vertx-db2-client)), but it is a one-off API for DB2, so the driver is not usable by frameworks like [Spring Data R2DBC](https://spring.io/projects/spring-data-r2dbc).

## Spring
This project also provides a [Spring R2DBC dialect](src/main/com/usaa/reactive/r2dbc/spring/DB2R2dbcDialect.java) so that this driver can be used with Spring Boot, but does not bring any Spring libraries along as transitive dependencies. This way, presence of this core R2DBC driver will not pollute the classpath of a non-Spring application. 

For Spring applications, this library depends on the `getConverters()` method on [Db2Dialect](https://docs.spring.io/spring-data/jdbc/docs/current/api/org/springframework/data/relational/core/dialect/Db2Dialect.html) so must be used with Spring Data Relational 2.3.0 or above (Spring Boot 2.6.x). Using this driver with earlier Spring versions will result in a `NoSuchMethodError`.


# Known Issues
## Spring Data type converters
There may be some data types that Spring doesn't yet know how to convert between the Java type returned by a DB query and the Java type on an entity class. For example, on DB2 LUW there is a `BOOLEAN` SQL type, which (for some reason) is returned as a Java `Short` in rows retrieved from the DB. The [BooleanConverter](src/main/java/com/usaa/reactive/r2dbc/spring/BooleanConverter.java) class was added to support mapping from a `BOOLEAN` column on a table to a `Boolean` property on an entity class. There will likely be more data types that need converters added to the dialect - if you bump into one, please report an issue with your Java type and DB2 type so that a converter can be created for that type. This is a sample of what exception message to look for:
```
org.springframework.data.mapping.MappingException: Could not read property @org.springframework.data.relational.core.mapping.Column(value=)private boolean com.usaa.reactive.r2dbc.spring.integration.tictactoe.GameStats.over from column over!
	at org.springframework.data.r2dbc.convert.MappingR2dbcConverter.readFrom(MappingR2dbcConverter.java:187)
	at org.springframework.data.r2dbc.convert.MappingR2dbcConverter.read(MappingR2dbcConverter.java:138)
	at org.springframework.data.r2dbc.convert.MappingR2dbcConverter.read(MappingR2dbcConverter.java:121)
	at org.springframework.data.r2dbc.convert.EntityRowMapper.apply(EntityRowMapper.java:46)
	at org.springframework.data.r2dbc.convert.EntityRowMapper.apply(EntityRowMapper.java:29)
	at com.usaa.reactive.r2dbc.db2.DB2Result$ResultView.lambda$null$9(DB2Result.java:150)
	at reactor.core.publisher.FluxMapFuseable$MapFuseableSubscriber.onNext(FluxMapFuseable.java:113)
	... 72 more
Caused by: org.springframework.core.convert.ConverterNotFoundException: No converter found capable of converting from type [java.lang.Short] to type [boolean]
	at org.springframework.core.convert.support.GenericConversionService.handleConverterNotFound(GenericConversionService.java:322)
	at org.springframework.core.convert.support.GenericConversionService.convert(GenericConversionService.java:195)
	at org.springframework.core.convert.support.GenericConversionService.convert(GenericConversionService.java:175)
	at org.springframework.data.r2dbc.convert.MappingR2dbcConverter.getPotentiallyConvertedSimpleRead(MappingR2dbcConverter.java:280)
	at org.springframework.data.r2dbc.convert.MappingR2dbcConverter.readValue(MappingR2dbcConverter.java:204)
	at org.springframework.data.r2dbc.convert.MappingR2dbcConverter.readFrom(MappingR2dbcConverter.java:184)
	... 78 more
```

## BLOB / CLOB support
The underlying Vert.X driver does not support BLOB or CLOB types (i.e. won't send data for an INSERT, or read data from a SELECT in a streaming fashion). This driver "pretends" to support R2DBC `Blob` and `Clob` types by simply loading the entire LOB into memory (as a `byte[]` or `String`) and passing that down to the Vert.X driver. If you are inserting into a table with a LOB column, you will need to add a `CAST` to your values list so that the Vert.X driver transfers your data to the server as a simple `VARBINARY`/`VARCHAR` (which the DB server will then coerce into the appropriate LOB type when storing in the table).

So, be aware that reading/writing large LOB values will have a significant impact on memory utilization in your application. LOB values are **NOT PROCESSED IN A STREAMING MANNER** and won't be until the underlying Vert.X driver [adds BLOB/CLOB support](https://github.com/eclipse-vertx/vertx-sql-client/issues/496)

### TL;DR
 - Anticipate lots of Heap utilization when working with LOB's
 - Write your INSERT queries like this:
   ```sql
   INSERT INTO blob_test ( my_cool_blob_column, other_column ) VALUES ( CAST(? AS VARBINARY), ? )
   INSERT INTO clob_test ( my_cool_clob_column, other_column ) VALUES ( CAST(? AS VARCHAR),   ? )
   ```
 - Write your SELECT queries like this:
   ```sql
   SELECT CAST(my_cool_blob_column AS VARBINARY) AS my_cool_blob_column, other_column FROM blob_test
   SELECT CAST(my_cool_clob_column AS VARCHAR)   AS my_cool_clob_column, other_column FROM clob_test
   ```



# License
This driver is Open Source software released under the [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0.html).


All trademarks are the property of their respective owners
