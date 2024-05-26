# spotify-to-aws-to-snowflake-project

ETL Pipeline: Spotify API > AWS [CloudWatch trigger > Lambda (Python) > S3] > Snowflake [Snowpipe]

### Architecture Diagram

![Architecture Diagram](https://raw.githubusercontent.com/rokusho235/spotify-to-aws-to-snowpipe-project/main/aws-snowflakePipeline.png)

### Services Used

1.  [**Spotify API**](https://developer.spotify.com/documentation/web-api) - Used API to pull album, artist, and song data from an emo playlist
2.  **AWS Lambda, CloudWatch, S3**
3.  **Snowflake Snowpipe**
