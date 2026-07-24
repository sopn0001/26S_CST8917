#!/usr/bin/env pwsh
#
# Provisions everything the demo needs, then prints a ready-to-paste .env
#
# Prerequisites:  az login   (and an active subscription)
# Runtime:        ~4 minutes. Do this BEFORE the session, not during it.
#
# Cost note: Service Bus Standard and Event Hubs Basic are inexpensive but
# not free. Run infra/cleanup.ps1 when you are finished.

$ErrorActionPreference = 'Stop'

# ---- names -----------------------------------------------------------------
$SUFFIX   = if ($env:SUFFIX) { $env:SUFFIX } else { Get-Random -Maximum 32768 }
$LOCATION = if ($env:LOCATION) { $env:LOCATION } else { 'canadacentral' }
$RG       = if ($env:RG) { $env:RG } else { 'rg-eda-demo' }

$STORAGE  = "edademo$SUFFIX"
$SB_NS    = "sb-eda-demo-$SUFFIX"
$EH_NS    = "ehns-eda-demo-$SUFFIX"
$EG_TOPIC = "egt-eda-demo-$SUFFIX"

$ORDER_QUEUE          = 'orders'
$SESSION_QUEUE        = 'orders-sessions'
$STATUS_TOPIC         = 'order-status'
$RECEIPT_QUEUE        = 'receipts'
$EVENTHUB             = 'telemetry'
$CHECKPOINT_CONTAINER = 'checkpoints'
$DEADLETTER_CONTAINER = 'eventgrid-deadletter'

function Say([string]$msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor White
}

# ---- resource group --------------------------------------------------------
Say "Resource group: $RG ($LOCATION)"
az group create --name $RG --location $LOCATION --output none

# ---- storage: queues + checkpoints + event grid dead letters ---------------
Say "Storage account: $STORAGE"
az storage account create `
    --name $STORAGE --resource-group $RG --location $LOCATION `
    --sku Standard_LRS --kind StorageV2 --output none

$STORAGE_CONN = az storage account show-connection-string `
    --name $STORAGE --resource-group $RG --query connectionString -o tsv

az storage queue create --name $RECEIPT_QUEUE `
    --connection-string $STORAGE_CONN --output none
az storage container create --name $CHECKPOINT_CONTAINER `
    --connection-string $STORAGE_CONN --output none
az storage container create --name $DEADLETTER_CONTAINER `
    --connection-string $STORAGE_CONN --output none

# ---- service bus -----------------------------------------------------------
# Standard tier is required for topics and subscriptions. Sessions and
# duplicate detection are also Standard+.
Say "Service Bus namespace: $SB_NS (Standard)"
az servicebus namespace create `
    --name $SB_NS --resource-group $RG --location $LOCATION `
    --sku Standard --output none

Say "  queue: $ORDER_QUEUE"
az servicebus queue create `
    --name $ORDER_QUEUE --namespace-name $SB_NS --resource-group $RG `
    --max-delivery-count 5 `
    --enable-dead-lettering-on-message-expiration true `
    --default-message-time-to-live PT1H `
    --lock-duration PT30S `
    --output none

Say "  queue: $SESSION_QUEUE (sessions enabled)"
az servicebus queue create `
    --name $SESSION_QUEUE --namespace-name $SB_NS --resource-group $RG `
    --enable-session true --max-delivery-count 5 --output none

Say "  topic: $STATUS_TOPIC"
az servicebus topic create `
    --name $STATUS_TOPIC --namespace-name $SB_NS --resource-group $RG `
    --output none

# Subscription 1: everything.
az servicebus topic subscription create `
    --name notify --topic-name $STATUS_TOPIC `
    --namespace-name $SB_NS --resource-group $RG `
    --max-delivery-count 5 --output none

# Subscription 2: large orders only. The filter runs in the broker, so this
# subscription is never delivered the small ones at all.
az servicebus topic subscription create `
    --name audit --topic-name $STATUS_TOPIC `
    --namespace-name $SB_NS --resource-group $RG `
    --max-delivery-count 5 --output none

# Replace the default $Default rule (1=1) with a SQL filter.
az servicebus topic subscription rule delete `
    --name '$Default' --subscription-name audit --topic-name $STATUS_TOPIC `
    --namespace-name $SB_NS --resource-group $RG --output none 2>$null

az servicebus topic subscription rule create `
    --name large-orders --subscription-name audit --topic-name $STATUS_TOPIC `
    --namespace-name $SB_NS --resource-group $RG `
    --filter-sql-expression "amount > 13" --output none

$SB_CONN = az servicebus namespace authorization-rule keys list `
    --namespace-name $SB_NS --resource-group $RG `
    --name RootManageSharedAccessKey --query primaryConnectionString -o tsv

# ---- event hubs ------------------------------------------------------------
# 4 partitions: enough to demonstrate ordering and the parallelism ceiling.
# Partition count is FIXED at creation on Basic/Standard — choose deliberately.
Say "Event Hubs namespace: $EH_NS"
az eventhubs namespace create `
    --name $EH_NS --resource-group $RG --location $LOCATION `
    --sku Standard --output none

Say "  hub: $EVENTHUB (4 partitions, 1 day retention)"
az eventhubs eventhub create `
    --name $EVENTHUB --namespace-name $EH_NS --resource-group $RG `
    --partition-count 4 --cleanup-policy Delete --retention-time-in-hours 24 `
    --output none

# Extra consumer groups so students can see independent readers.
foreach ($cg in @('archive', 'fraud')) {
    az eventhubs eventhub consumer-group create `
        --name $cg --eventhub-name $EVENTHUB `
        --namespace-name $EH_NS --resource-group $RG --output none
}

$EH_CONN = az eventhubs namespace authorization-rule keys list `
    --namespace-name $EH_NS --resource-group $RG `
    --name RootManageSharedAccessKey --query primaryConnectionString -o tsv

# ---- event grid ------------------------------------------------------------
Say "Event Grid custom topic: $EG_TOPIC"
az eventgrid topic create `
    --name $EG_TOPIC --resource-group $RG --location $LOCATION `
    --output none

$EG_ENDPOINT = az eventgrid topic show `
    --name $EG_TOPIC --resource-group $RG --query endpoint -o tsv
$EG_KEY = az eventgrid topic key list `
    --name $EG_TOPIC --resource-group $RG --query key1 -o tsv

# ---- output ----------------------------------------------------------------
$ENV_FILE = Join-Path $PSScriptRoot '..' | Join-Path -ChildPath '.env'
$TIMESTAMP = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

$envContent = @"
# Generated by infra/provision.ps1 on $TIMESTAMP
# Resource group: $RG

STORAGE_CONNECTION_STRING=$STORAGE_CONN
RECEIPT_QUEUE=$RECEIPT_QUEUE
CHECKPOINT_CONTAINER=$CHECKPOINT_CONTAINER

SERVICEBUS_CONNECTION_STRING=$SB_CONN
ORDER_QUEUE=$ORDER_QUEUE
SESSION_QUEUE=$SESSION_QUEUE
STATUS_TOPIC=$STATUS_TOPIC
NOTIFY_SUBSCRIPTION=notify
AUDIT_SUBSCRIPTION=audit

EVENTHUB_CONNECTION_STRING=$EH_CONN
EVENTHUB_NAME=$EVENTHUB
EVENTHUB_CONSUMER_GROUP=`$Default

EVENTGRID_TOPIC_ENDPOINT=$EG_ENDPOINT
EVENTGRID_TOPIC_KEY=$EG_KEY
"@

Set-Content -Path $ENV_FILE -Value $envContent -NoNewline

Say "Done. Wrote $ENV_FILE"
Write-Host ""
Write-Host "  Resource group : $RG"
Write-Host "  Suffix         : $SUFFIX"
Write-Host ""
Write-Host "  Next:  pip install -r requirements.txt"
Write-Host "         python 01_storage_queue/producer.py"
Write-Host ""
Write-Host "  When finished:  `$env:RG='$RG'; ./infra/cleanup.ps1"
Write-Host ""
