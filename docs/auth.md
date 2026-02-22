# Authentication for Budget Reconciliation

## Purpose

- Authenticate **Google Cloud Platform** services used in this pipeline

- Authenticate **Google Sheets API** for reading budget allocation

- Use manual login with **Application Default Credentials** for local environment

- Use **Service Account** authentication to manage permissions in cloud environments

- Use centralized Google Cloud Project with required APIs enabled for cloud deployment

---

## Local setup

### Install Google Cloud SDK

- Download and install Google Cloud SDK from official source
```bash
https://cloud.google.com/sdk
```

- Verify installed Google Cloud SDK version
```bash
gcloud --version
```

---

### Login to Google Cloud using Application Default Credentials

- Login to Google Cloud on local environment
```bash
gcloud auth login
```

- Set default Google Cloud project for Google BigQuery and quota billing
```bash
gcloud config set project YOUR_GOOGLE_CLOUD_PROJECT_ID
```

- Check quota project attached to ADC
```bash
gcloud auth application-default show-quota-project
```

- Verify Google Cloud quota project
```bash
gcloud config get-value project
```

---

### Login to Google Sheets using Application Default Credentials

- Activate Google Sheets API on Google Cloud Platform's API and Services

- Login to Google Sheet on local environment
```bash
gcloud auth application-default login `
  --scopes="https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/cloud-platform"
```

---

## Cloud Run setup

### Enable minimum required APIs and services

- Enable **Cloud Run API** for container execution in the target Google Cloud project

- Enable **Cloud Run API** for container execution in the target Google Cloud project

- Enable **Google BigQuery API** for data warehouse access in the target Google Cloud project

- Enable **Google Sheets API** for reading budget file in the target Google Cloud project

---

### Enable Service Account

- Create a dedicated Google Cloud Platform's **Service Account** for pipeline_recon_ads

- Grant **Cloud Run Admin permissions** for required IAM Roles

- Grant **BigQuery Data Editor** and **BigQuery Job User** for required IAM Roles

---

### Share Google Sheets access

- Open the Google Sheet containing budget allocation

- Click Share button on the top right

- Add Service Account email `pipeline-recon-ads@YOUR_PROJECT_ID.iam.gserviceaccount.com`

- Grant `Viewer` if read-only or `Editor` permission if writing back reconciliation results