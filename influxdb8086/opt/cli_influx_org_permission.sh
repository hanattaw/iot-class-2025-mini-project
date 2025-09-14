#!/bin/bash
# ⚡ Auto create InfluxDB org, user, bucket, and token for students
# ✅ Export tokens to student_tokens.csv
# ✅ Install jq if not present

# ติดตั้ง jq (สำหรับ Ubuntu/Debian)
if ! command -v jq &> /dev/null; then
  echo "🔧 Installing jq..."
  apt update && apt install -y jq
fi

# Student IDs
students=(
# 6310301030
# 6410301001
# 6410301032
# 6510301002
# 6510301003
# 6510301004
# 6510301007
# 6510301010
# 6510301011
# 6510301012
# 6510301013
6510301014
6510301015
6510301016
6510301017
6510301018
6510301019
6510301023
6510301024
6510301025
6510301026
# 6510301029
# 6510301032
# 6510301033
# 6510301035
# 6510301041
# 6510301042
# 6510301044
# 6510301046
# 6510301047
# 6510301048
# 6520301001
# 6520301002
)

# Clear old CSV
OUTPUT="student_tokens_8086_org_permission.csv"
echo "student_id,org,bucket,password,token" > $OUTPUT

for sid in "${students[@]}"; do
  ORG="org-${sid}"
  USER="${sid}"
  BUCKET="bucket-${sid}"
  PASSWORD="pw${sid}"

  echo "=== Processing $sid ==="

  # 1. Create token and capture it
  # influx auth create --user 6510301014  --org org-6510301014 --all-access --description "Allow user to create buckets"
  TOKEN=$(influx auth create \
    --user $USER \
    --org $ORG \
    --all-access \
    --description "Allow user to create buckets" \
    --json | jq -r '.token')

  
  # 2. Append to CSV
  echo "$sid,$ORG,$BUCKET,$PASSWORD,$TOKEN" >> $OUTPUT

  echo "✅ Created org=$ORG, user=$USER, bucket=$BUCKET, token saved"
done

echo "🎉 All done! Tokens exported to $OUTPUT"
