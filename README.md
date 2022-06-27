This is an example of my ETL processes made by Spark on Scala.

This ETL process processes data on tax transactions.
We take the initial dataframe (taxTransactionDf) and apply filter rules which are stored in another dataframe. 
Each rule should be applied to the initial dataframe in parallel and then they should be united into one final dataframe

Here is schema of ETL 

![rules](https://user-images.githubusercontent.com/79256653/175340815-cb5d8c60-ca69-4c47-a2fb-b934a5b8649f.png)


Inside you can find the next objects and classes:

1.0 srs/main/scala

1.1 Application

Main app. It accepts spark arguments and addition arguments startDate and endDate. 
This information is needed to perform the correct ETL operations. Concurrentthought.cla library is used to parse these arguments.

1.2 ApplicationArguments

This class is needed for Concurrentthought.cla to create new arguments for the main app

1.3 Const

Constants which are used in this ETL process

1.4 DataSources

Here you can find methods which read initial dataframes

1.5 AutoKnuRule
Here you can find the transform method which performs all aggregation functions

2.0 srs/main/test

2.1 KnuTest

Here are the tests. We know that transactions with certain masks should pass all filters and aggregations and stay in the final dataframe.
We check all combinations of masks. If we get an empty dataframe after the ETL process it means that the test is failed.
