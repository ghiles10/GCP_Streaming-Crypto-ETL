# Infrastructure as code
Terraform is used to provision and manage GCP services required for the pipeline.
By using infrastructure as code, the deployment and management of the project's resources become more streamlined, allowing for easier maintenance and updates.


## Setup Google Cloud

- Download service-account-keys (.json) 
- Rename the .json key file to google_credentials.json

```ini
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/google_credentials.json"

# verify authentication
gcloud auth application-default login
```

``` ini 
git clone https://github.com/ghiles10/GCP_Streaming-Crypto-ET.git && \
cd GCP_Streaming-Crypto-ET/terraform
```

- Initialisation 

``` ini 
terraform init
```

- Plan 

``` ini
terraform plan
``` 

- Apply 

``` ini 
terraform apply
```
