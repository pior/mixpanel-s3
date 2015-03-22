# mixpanel-s3

> Extract Mixpanel raw events and upload to S3

## Features

- Static binary (Go)
- Download raw events from the Mixpanel data api
- Upload on S3
- On the fly Gzip compression
- Split by events

## TODO 

- Retry download (5 times, wait 60s before retrying)

## Usage

    usage: mixpanel-s3 --key=XXXXXX --secret=XXXXXX --bucket=BUCKET [<flags>]

    Extract Mixpanel raw events and upload to S3

    Flags:
      --help               Show help.
      -f, --from="2015-03-21"  
                           Extract from this date
      -t, --to="2015-03-21"  
                           Extract to this date
      -k, --key=XXXXXX     Mixpanel api key
      -s, --secret=XXXXXX  Mixpanel secret key
      -b, --bucket=BUCKET  S3 bucket name
      -p, --prefix=PREFIX  S3 key prefix
      --split              Split by event name
      --version            Show application version.

> Note: use the TMPDIR to control where mixpanel-s3 will stage the raw events

## Environment Variables

- MIXPANEL_API_KEY
- MIXPANEL_SECRET_KEY
- S3_BUCKET
- S3_PREFIX
- TMPDIR
