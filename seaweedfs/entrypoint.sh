#!/bin/sh
set -eu

cat > /tmp/s3.json <<EOF
{
  "identities": [
    {
      "name": "admin-bootstrap",
      "credentials": [
        {
          "accessKey": "${S3_ADMIN_ACCESS_KEY}",
          "secretKey": "${S3_ADMIN_SECRET_KEY}"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "List",
        "Write",
        "Tagging"
      ]
    },
    {
      "name": "ingestion",
      "credentials": [
        {
          "accessKey": "${S3_ACCESS_KEY}",
          "secretKey": "${S3_SECRET_KEY}"
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

exec /usr/bin/weed server -s3 -s3.iam=false -dir=/data -s3.config=/tmp/s3.json
