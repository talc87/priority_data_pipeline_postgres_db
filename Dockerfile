FROM python:3.10.4-slim-buster

# Install PostgreSQL development libraries since they are not part of the image "python:3.10.4-slim-buster"
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

    
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt
EXPOSE 5000



# Defining Environment variables:

# SQL DB connection str:
ENV sqlConnStr=${mongoDbConnStr}
# Mongo DB connection str:
ENV mongoDbConnStr=${mongoDbConnStr}

# Hardcoded env varibles-NO NEED TO MODIFY
ENV metadataDbName="metadataDB"
ENV configDbName="priority_dwh_admin"
ENV configCollectionName="configCollection"
ENV datatypeMappingCollectionName="datatypeMapping"



# run the flask app
CMD ["python", "app.py"]