#!/bin/sh
set -eu

cat > /tmp/s3.json <<EOF
{
  "identities": [
    {
      "name": "ingestion",
      "credentials": [
        {
          "accessKey": "$"+"{S3_ACCESS_KEY}",
          "secretKey": "$"+"{S3_SECRET_KEY}"
        }
      ],
      "actions": [
        "Read:elt-raw",
        "Write:elt-raw"
      ]
    }
  ]
}
EOF

exec /usr/bin/weed server -s3 -dir=/data -s3.config=/tmp/s3.json
