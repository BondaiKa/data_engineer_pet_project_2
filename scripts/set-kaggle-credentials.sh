ENV_FILE_PATH="env/.env"

if [ -f $ENV_FILE_PATH ]; then
  echo "0. Export env..."
  export $(cat $ENV_FILE_PATH | grep -v '#' | sed 's/\r$//' | awk '/=/ {print $1}')
fi