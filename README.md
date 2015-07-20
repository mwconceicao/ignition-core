# Search-Ignition

This project provides all the jobs necessary for the Search Product.

Jobs Available:

- SitemapXMLSetup
- TransactionETLSetup
- MainIndicatorsSetup
- TopQueriesSetup
- SearchETLSetup
- ValidQueriesSetup

## Getting Started - How to run one Job?

- Check if you have Java 7
- Clone the repo with --recursive
- `./sbt update && ./sbt compile`
- Create the cluster with the following command (remember to change valid-queries-dev to a proper name and set the right
amount of machines instead of 5 if you please):
```bash
core/tools/cluster.py launch valid-queries-dev 5 -i d2.2xlarge --spot-price 0.9 --worker-instances 2  \
   --master-instance-type m3.2xlarge --security-group Ignition --zone us-east-1b --vpc vpc-d92a61bc   \
   --vpc-subnet subnet-d3a511f8
```
- Run your Job =):
```bash
core/tools/cluster.py jobs run valid-queries-dev ValidQueriesSetup 20G
```
- Destroy your cluster (time is money):
```bash
core/tools/cluster.py destroy valid-queries-dev
```

All jobs are "self-contained" if your job runs properly you don't need to do anything else. It will send the data to the
right place: DashboardAPI, Elasticsearch or S3.

## Tests

First, clone everything like stated above. Run the tests with:
```
./sbt test
```

If you want to run a single job, you can use:
```
./sbt testOnly *ValidQ*
```
for running ValidQueries tests.

# How do I run it for a time frame other than the default, i.e. arbitrary time intervals?

FIXME: You can't. Ignition Core does not support it yet.

# Sometimes you have the output on S3 and you want to upload it? New cluster?

## TODO: EXPLAIN UPLOADER

## Configuration

You should set the proper configuration: user and password for the dashboard api, and the elasticsearch cluster name and
endpoint.

```conf
akka {
  loglevel = INFO
  log-dead-letters = on
  log-dead-letters-during-shutdown = on
  loggers = ["akka.event.slf4j.Slf4jLogger"]
}

dashboard-api {
  user = "SOMETHING"
  password = "SOMETHING"
  url = "https://dashboard-api.chaordicsystems.com"
}

elastic-search {
  cluster-name = ""
  url = ""
  port= 9200
}
```

# About the jobs

## SitemapXMLSetup

Provide the generation of sitemap.xml. It is controlled by the SearchCentral. It fetches all configured
clients on SearchCentral and its configurations. The output consists of some files on s3: sitemap.xml that is an index
for other files on s3. These files contains the actual links. Example of configuration:

```json
{...
  "sitemap": {
    "numberPartFiles": 10,                  // Number of part files to split the output
    "useDetails": false,                    // Explode links creating filters
    "maxSearchItems": 100000,               // Number of search log based output, top 100k popular queries
    "details": [                            // The details to use with the option `useDetails`
      "publisher",
      "brand",
      "ano",
      "produtoDigital"
    ],
    "generatePages": true,                  // Generate links for pages?
    "generateSearch": true                  // Generate links for search?
  }
}
```

## TransactionETLSetup

Calculate metrics about sales. It need the tagging done by Data Collection. The output is automatically sent to
DashboardAPI. It is responsible for calculating the metrics:

- sales_search: Aggregation of the money spent on all sales that search participated;
- sales_overall: Aggregation of money spent on all sales.

## MainIndicatorsSetup

Calculate metrics about searches and clicks. It is responsible for calculating the following metrics:

- searches: count of all valid searches.
- unique_searches: count of all unique searches.
- search_clicks: count of all valid clicks.
- unique_search_clicks: count of all unique valid clicks.
- autocomplete_count: count of all valid autocomplete.
- autocomplete_unique: count of all valid unique autocomplete.
- autocomplete_clicks: count of all valid autocomplete clicks.
- unique_autocomplete_clicks: count of all unique autocomplete clicks.

For detailed explanation check the file: "MainIndicators.scala" it contains detailed explanation of what the job do and
what is considered a valid search/click. The result is automatically sent to DashboardAPI

## TopQueriesSetup

Calculate Top Queries, i.e., the most searched queries with and without results. It is used to display analytics for our
client. The result is saved on s3 and is sent to Elasticsearch.

## SearchETLSetup

Full Job. It aggregate TransactionETL, MainIndicators and Top Queries. Save all output to s3 and then sent it to
DashboardAPI/Elasticsearch.

## ValidQueriesSetup

Calculate ValidQueries, it is used to create autocomplete and query suggestions for our users. The result is saved to
S3 and then sent to Elasticsearch. For detailed information check the file ValidQueriesJob.scala.

# Deploy

Check [search-ansible](https://github.com/chaordic/search-ansible)
