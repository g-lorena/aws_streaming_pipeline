## AWS Streaming Pipeline for real-time Cryptocurrency Price analysis

This AWS streaming pipeline ingests real-time Cryptocurrency price data from the CoinGecko API, transform it for analysis and stores it in a ready-to-use format. The pipeline leverage the following services:
- **AWS Kinesis and Firehose**: Continuously streams data from the CoinGecko API in real-time.I also implement data partitioning within Firehose to improve performance. 
- **Amazon S3**: Serves as the data lake for storing the raw cryptocurrency price data.
- **AWS Glue**: Provides a job to transform the raw data into a schema optimized for analytics.
- **AWS Lambda**: Acts as an event-driven trigger, initiating the Glue job whenever new data arrives in the S3 raw layer.
- **Amazon SNS**: Publishes notifications (in SNS's topic) about new data arrivals in S3, which are then picked up by the Lambda function.

This architecture ensures that the pipeline automatically processes incoming data, keeping the analytical layer up-to-date with the lastest information. 

![Pipeline Architecture](images/architecture.png)

For more information about each part : Data ingestion, Data transformation and orchestration, you can refer to theses articles I wrote: 
- [My Streamlining Data flow: Kinesis, Firehose & S3](https://medium.com/data-engineer-things/my-streamlining-data-flow-kinesis-firehose-s3-ea2566e2851e?sk=5cc23f626d0782caa8bb1fe642f941b4)
- [How I automate my Glue Job using AWS SNS and AWS Lambda](https://medium.com/art-of-data-engineering/how-i-automate-my-glue-job-using-aws-sns-and-aws-lambda-0f1cff9841d1)
- [How I Transform S3 Data with AWS Glue Jobs](https://medium.com/data-engineer-things/how-i-transform-s3-data-with-aws-glue-jobs-718f0c27b7c6)