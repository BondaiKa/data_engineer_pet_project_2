FOLDER="scripts"

Echo "1. Set kaggle credentials..."
bash "$FOLDER"/set-kaggle-credentials.sh

Echo "2.Download yelp dataset..."
python3 data-engineer-pet-project-cli.py load-yelp-dataset-locally-cli