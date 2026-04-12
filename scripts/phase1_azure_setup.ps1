# =============================================================================
# Phase 1 — Azure Foundation Setup
# Run this in PowerShell after: az login
#
# Before running:
#   1. Replace <YOUR_SUBSCRIPTION_ID> with your actual subscription ID
#   2. Run: az login
#   3. Run: .\scripts\phase1_azure_setup.ps1
# =============================================================================

$SUBSCRIPTION_ID = "<your-subscription-id>"    # <-- paste from portal.azure.com
$RESOURCE_GROUP  = "rg-<your-project>-dev"     # <-- must be globally unique
$LOCATION        = "eastus"
$ADLS_ACCOUNT    = "<your-adls-account>"        # <-- must be globally unique, lowercase, 3-24 chars
$CONTAINER       = "telecom"
$KEY_VAULT       = "kv-<your-project>-dev"      # <-- must be globally unique

Write-Host "`n[1/6] Setting subscription..." -ForegroundColor Cyan
az account set --subscription $SUBSCRIPTION_ID
az account show --query "{subscription:name, id:id}" -o table

Write-Host "`n[2/6] Creating resource group..." -ForegroundColor Cyan
az group create `
  --name $RESOURCE_GROUP `
  --location $LOCATION `
  --tags project=telecom-triage env=dev

Write-Host "`n[3/6] Creating ADLS Gen2 storage account..." -ForegroundColor Cyan
az storage account create `
  --name $ADLS_ACCOUNT `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --sku Standard_LRS `
  --kind StorageV2 `
  --enable-hierarchical-namespace true `
  --min-tls-version TLS1_2 `
  --allow-blob-public-access false

Write-Host "`n[4/6] Creating ADLS container..." -ForegroundColor Cyan
az storage fs create `
  --name $CONTAINER `
  --account-name $ADLS_ACCOUNT `
  --auth-mode login

Write-Host "`n[5/6] Creating Key Vault..." -ForegroundColor Cyan
az keyvault create `
  --name $KEY_VAULT `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --sku standard `
  --enable-rbac-authorization false

Write-Host "`n[6/6] Storing ADLS key in Key Vault..." -ForegroundColor Cyan
$ADLS_KEY = az storage account keys list `
  --account-name $ADLS_ACCOUNT `
  --resource-group $RESOURCE_GROUP `
  --query "[0].value" -o tsv

az keyvault secret set `
  --vault-name $KEY_VAULT `
  --name "adls-key" `
  --value $ADLS_KEY

Write-Host "`nPhase 1 complete." -ForegroundColor Green
Write-Host "Resource group:   $RESOURCE_GROUP"
Write-Host "ADLS account:     $ADLS_ACCOUNT"
Write-Host "Container:        $CONTAINER"
Write-Host "Key Vault:        $KEY_VAULT"
Write-Host "`nNext: Copy ADLS key to .env file:"
Write-Host "  ADLS_KEY = $ADLS_KEY" -ForegroundColor Yellow
