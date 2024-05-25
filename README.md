# spotify-to-aws-to-snowflake-project

ETL Pipeline: Spotify API > AWS [CloudWatch trigger > Lambda (Python) > S3] > Snowflake [Snowpipe]

### Architecture Diagram

![Architecture Diagram](https://media.licdn.com/dms/image/D4E22AQElmJVBRVcLVQ/feedshare-shrink_1280/0/1699018039964?e=1717027200&v=beta&t=tuUnJa6i-A0aS41z6puuqIw7pncKZx77dLDAU1SMq3E)

### Services Used

1.  [**Spotify API**](https://developer.spotify.com/documentation/web-api) - Used API to pull album, artist, and song data from an emo playlist
2.  **AWS Lambda**
3.  **AWS CloudWatch**
4.  **Snowflake Snowpipe**
