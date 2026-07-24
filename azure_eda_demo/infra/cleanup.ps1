#!/usr/bin/env pwsh
#
# Deletes everything provision.ps1 created.
# Run this when the session is over — Service Bus Standard and Event Hubs
# Standard bill per hour whether or not you are using them.

$ErrorActionPreference = 'Stop'

$RG = if ($env:RG) { $env:RG } else { 'rg-eda-demo' }

Write-Host ""
Write-Host "This will DELETE the resource group '$RG' and everything in it."
$CONFIRM = Read-Host "Type the resource group name to confirm"

if ($CONFIRM -ne $RG) {
    Write-Host "Names did not match. Nothing was deleted."
    exit 1
}

az group delete --name $RG --yes --no-wait
Write-Host ""
Write-Host "Deletion started in the background. Check with:"
Write-Host "  az group show --name $RG"
Write-Host ""
